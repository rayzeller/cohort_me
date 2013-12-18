require "cohort_me/version"

module CohortMe

  def self.analyze(options={})

    start_from_interval = options[:start_from_interval] || 12
    interval_name = options[:period] || "weeks"
    activation_class = options[:activation_class] 
    activation_table_name = ActiveModel::Naming.plural(activation_class)
    activation_user_id = options[:activation_user_id] || "user_id"
    activation_conditions = options[:activation_conditions] 
    activity_conditions = options[:activity_conditions] 
    activation_time_column = options[:activation_time_column] || "created_at"

    activity_class = options[:activity_class] || activation_class
    activity_table_name = ActiveModel::Naming.plural(activity_class)
    activity_user_id = options[:activity_user_id] || "user_id"

    summary_column = options[:summary_column] || "count(*)"

    period_values = %w[weeks days months]

    raise "Period '#{interval_name}' not supported. Supported values are #{period_values.join(' or ')}" unless period_values.include? interval_name

    start_from = nil
    time_conversion = nil
    cohort_label = nil
    day_offset = (options[:day_offset] || 0).to_i

    if interval_name == "weeks"
      start_from = start_from_interval.weeks.ago.beginning_of_week + day_offset.days
      time_conversion = 604800
    elsif interval_name == "days"
      start_from = start_from_interval.days.ago.beginning_of_day
      time_conversion = 86400
    elsif interval_name == "months"
      start_from = start_from_interval.months.ago.beginning_of_month + day_offset.days
      time_conversion = 1.month.seconds
    end

    activation_conversion = self.convert_to_cohort_date_in_postgres("#{activation_table_name}.#{activation_time_column}", start_from, time_conversion)
    activity_conversion = self.convert_to_cohort_date_in_postgres("#{activity_table_name}.created_at", start_from, time_conversion)

    cohort_query = activation_class.select("#{activation_table_name}.#{activation_user_id}, MIN(#{activation_conversion}) as cohort_date, MIN(#{activation_table_name}.#{activation_time_column}) as cohort_date_exact").where("#{activation_table_name}.#{activation_time_column} >= ?", start_from).where("#{activation_conversion} < ?", start_from + (start_from_interval * time_conversion).seconds)
    activated_query = activation_class.select("count(*) number_activated, #{activation_conversion} cohort_date").where("#{activation_table_name}.#{activation_time_column} >= ?", start_from).where("#{activation_conversion} < ?", start_from + (start_from_interval * time_conversion).seconds)

    if activation_conditions
      activated_query = activated_query.where(activation_conditions)
      cohort_query = cohort_query.where(activation_conditions)
    end

    cohort_query = cohort_query.group("#{activation_user_id}")
    activated_query = activated_query.group("cohort_date")
    # if custom_selects
    #   cohort_query = cohort_query.select(custom_selects)
    # end

    if %(mysql mysql2).include?(ActiveRecord::Base.connection.instance_values["config"][:adapter])
      select_sql = "#{activity_table_name}.#{activity_user_id}, #{activity_table_name}.created_at, cohort_date, FLOOR(TIMEDIFF(#{activity_table_name}.created_at, cohort_date)/#{time_conversion}) as periods_out"
    elsif ActiveRecord::Base.connection.instance_values["config"][:adapter] == "postgresql"
      select_sql = "count(distinct #{activity_table_name}.#{activity_user_id}) count, cohort_date, FLOOR(extract(epoch from (#{activity_table_name}.created_at - cohort_date_exact))/#{time_conversion}) as periods_out"
      select_summary_sql = "#{summary_column} summary_value, cohort_date, FLOOR(extract(epoch from (#{activity_table_name}.created_at - cohort_date_exact))/#{time_conversion}) as periods_out"
      unique_summary_sql = "distinct orders.user_id as unique_value, cohort_date, FLOOR(extract(epoch from (#{activity_table_name}.created_at - cohort_date_exact))/#{time_conversion}) as periods_out"
    else
      raise "database not supported"
    end

    summary_query = activity_class.where("#{activity_table_name}.created_at >= ?", start_from).select(select_summary_sql).joins("JOIN (" + cohort_query.to_sql + ") AS cohorts ON #{activity_table_name}.#{activity_user_id} = cohorts.#{activation_user_id}").where("orders.created_at < cohort_date + #{time_conversion} * #{start_from_interval} * INTERVAL '1 SECOND'", time_conversion).where('cohort_date < ?',start_from + (start_from_interval*time_conversion).seconds).where('cohort_date <= ?',start_from + (start_from_interval*time_conversion).seconds).group("cohort_date,periods_out")
    unique_query = activity_class.where("#{activity_table_name}.created_at >= ?", start_from).select(unique_summary_sql).joins("JOIN (" + cohort_query.to_sql + ") AS cohorts ON #{activity_table_name}.#{activity_user_id} = cohorts.#{activation_user_id}").where("orders.created_at < cohort_date + #{time_conversion} * #{start_from_interval} * INTERVAL '1 SECOND'", time_conversion).where('cohort_date < ?',start_from + (start_from_interval*time_conversion).seconds).where('cohort_date <= ?',start_from + (start_from_interval*time_conversion).seconds).group("cohort_date,periods_out,user_id")
    data = activity_class.where("#{activity_table_name}.created_at >= ?", start_from).select(select_sql).joins("JOIN (" + cohort_query.to_sql + ") AS cohorts ON #{activity_table_name}.#{activity_user_id} = cohorts.#{activation_user_id}").where('cohort_date <= ?',start_from + (start_from_interval*time_conversion).seconds).where("orders.created_at < cohort_date + #{time_conversion} * #{start_from_interval} * INTERVAL '1 SECOND'").group("cohort_date,periods_out")
    
    if activity_conditions
      summary_query = summary_query.where(activity_conditions)
      data = data.where(activity_conditions)
    end
    
    # unique_data = data.all.uniq{|d| [d.send(activity_user_id), d.cohort_date, d.periods_out] }
    activated_data = Hash[activated_query.group_by{|d| convert_to_cohort_date(Time.parse(d.cohort_date.to_s), interval_name, day_offset)}]
    summary_data = Hash[summary_query.group_by{|d| convert_to_cohort_date(Time.parse(d.cohort_date.to_s), interval_name, day_offset)}]
    unique_data = Hash[unique_query.group_by{|d| convert_to_cohort_date(Time.parse(d.cohort_date.to_s), interval_name, day_offset)}]

    analysis = data.group_by{|d| convert_to_cohort_date(Time.parse(d.cohort_date.to_s), interval_name, day_offset)}

    cohort_hash =  Hash[analysis.sort_by { |cohort, data| cohort }]
    table = {}
    index = 0

    cohort_hash.each do |r| 
      periods = []
      summaries = []
      uniques = []

      table[r[0]] = {}

      cohort_hash.size.times{|i| periods << r[1].select{|d| d.periods_out.to_i == i}[0].count  if r[1].select{|d| d.periods_out.to_i == i}[0] } 
      cohort_hash.size.times{|i| summaries << summary_data[r[0]].select{|d| d.periods_out.to_i == i}[0].summary_value  if summary_data[r[0]].select{|d| d.periods_out.to_i == i}[0]} 
      cohort_hash.size.times{|i| uniques << unique_data[r[0]].select{|d| d.periods_out.to_i == i}.map(&:unique_value)  if unique_data[r[0]].select{|d| d.periods_out.to_i == i}[0]} 

      table[r[0]][:start] = activated_data[r[0]][0].number_activated
      

      table[r[0]][:count] = periods
      table[r[0]][:summary] = summaries
      table[r[0]][:unique] = uniques
      table[r[0]][:data] = r[1]
      index += 1
    end


    return table

  end

  def self.convert_to_cohort_date_in_postgres(column_name, start_time, interval_in_seconds)
    %Q{
      timestamp '#{start_time}'  at time zone 'US/Pacific' + 
      FLOOR(  
        extract(
          epoch from (#{column_name} - timestamp '#{start_time}'  at time zone 'US/Pacific')
          )/
          (
            #{interval_in_seconds}
          )
        )
       * INTERVAL '#{interval_in_seconds} SECOND'
    }
    # "date_trunc('#{interval.singularize}', #{column_name} at time zone 'UTC' at time zone 'US/Pacific' - interval '#{day_offset} DAY') + interval '#{day_offset} DAY'"
  end

  def self.convert_to_cohort_date(datetime, interval, day_offset = 0)
    # if interval == "weeks"
    #   return datetime.at_beginning_of_week.to_date + day_offset.days
      
    # elsif interval == "days"
      return Date.parse(datetime.strftime("%Y-%m-%d"))

    # elsif interval == "months"
      # return Date.parse(datetime.strftime("%Y-%m-1")) + day_offset.days
    # end
  end


end
