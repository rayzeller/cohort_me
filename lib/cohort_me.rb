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

    period_values = %w[weeks days months]

    raise "Period '#{interval_name}' not supported. Supported values are #{period_values.join(' or ')}" unless period_values.include? interval_name

    start_from = nil
    time_conversion = nil
    cohort_label = nil

    if interval_name == "weeks"
      start_from = start_from_interval.weeks.ago
      time_conversion = 604800
    elsif interval_name == "days"
      start_from = start_from_interval.days.ago
      time_conversion = 86400
    elsif interval_name == "months"
      start_from = start_from_interval.months.ago
      time_conversion = 1.month.seconds
    end

    activation_conversion = self.convert_to_cohort_date_in_postgres("#{activation_table_name}.#{activation_time_column}", interval_name)
    activity_conversion = self.convert_to_cohort_date_in_postgres("#{activity_table_name}.created_at", interval_name)

    cohort_query = activation_class.select("#{activation_table_name}.#{activation_user_id}, MIN(#{activation_conversion}) as cohort_date").group("#{activation_user_id}").where("#{activation_table_name}.{activation_time_column}" > ?", start_from)
    activated_query = activation_class.select("count(*) number_activated, #{activation_conversion} cohort_date").group("cohort_date")

    if activation_conditions
      activated_query = activated_query.where(activation_conditions)
      cohort_query = cohort_query.where(activation_conditions)
    end


    # if custom_selects
    #   cohort_query = cohort_query.select(custom_selects)
    # end

    if %(mysql mysql2).include?(ActiveRecord::Base.connection.instance_values["config"][:adapter])
    
      select_sql = "#{activity_table_name}.#{activity_user_id}, #{activity_table_name}.created_at, cohort_date, FLOOR(TIMEDIFF(#{activity_table_name}.created_at, cohort_date)/#{time_conversion}) as periods_out"
    elsif ActiveRecord::Base.connection.instance_values["config"][:adapter] == "postgresql"
      select_sql = "#{activity_table_name}.#{activity_user_id}, cohort_date, FLOOR(extract(epoch from (#{activity_table_name}.created_at - cohort_date))/#{time_conversion}) as periods_out"
      select_group_sql = "count(*) number_activities, cohort_date, FLOOR(extract(epoch from (#{activity_table_name}.created_at - cohort_date))/#{time_conversion}) as periods_out"
    else
      raise "database not supported"
    end
    activity_query = activity_class.where("#{activity_table_name}.created_at > ?", start_from).select(select_group_sql).joins("JOIN (" + cohort_query.to_sql + ") AS cohorts ON #{activity_table_name}.#{activity_user_id} = cohorts.#{activation_user_id}").group("cohort_date,periods_out")



    data = activity_class.where("#{activity_table_name}.created_at > ?", start_from).select(select_sql).joins("JOIN (" + cohort_query.to_sql + ") AS cohorts ON #{activity_table_name}.#{activity_user_id} = cohorts.#{activation_user_id}").group("cohort_date,periods_out,#{activity_table_name}.#{activity_user_id}")
    if activity_conditions
      activity_query = activity_query.where(activity_conditions)
      data = data.where(activity_conditions)
    end
    unique_data = data.all.uniq{|d| [d.send(activity_user_id), d.cohort_date, d.periods_out] }
    activated_data = Hash[activated_query.all.group_by{|d| convert_to_cohort_date(Time.parse(d.cohort_date.to_s), interval_name)}]
    activity_data = Hash[activity_query.all.group_by{|d| convert_to_cohort_date(Time.parse(d.cohort_date.to_s), interval_name)}]

    analysis = unique_data.group_by{|d| convert_to_cohort_date(Time.parse(d.cohort_date.to_s), interval_name)}

    cohort_hash =  Hash[analysis.sort_by { |cohort, data| cohort }]
    cohort_hash
    table = {}
    index = 0

    cohort_hash.each do |r| 
      periods = []
      activities = []

      table[r[0]] = {}

      cohort_hash.size.times{|i| periods << r[1].count{|d| d.periods_out.to_i == i}  if r[1]} 
      cohort_hash.size.times{|i| activities << activity_data[r[0]].select{|d| d.periods_out.to_i == i}[0].number_activities  if activity_data[r[0]].select{|d| d.periods_out.to_i == i}[0]} 

      table[r[0]][:start] = activated_data[r[0]][0].number_activated
      

      table[r[0]][:count] = periods
      table[r[0]][:active] = activities
      table[r[0]][:data] = r[1]
      index += 1
    end


    return table

  end

  def self.convert_to_cohort_date_in_postgres(column_name, interval)
    "date_trunc('#{interval.singularize}', #{column_name} at time zone 'UTC' at time zone 'US/Pacific')"
  end

  def self.convert_to_cohort_date(datetime, interval)
    if interval == "weeks"
      return datetime.at_beginning_of_week.to_date
      
    elsif interval == "days"
      return Date.parse(datetime.strftime("%Y-%m-%d"))

    elsif interval == "months"
      return Date.parse(datetime.strftime("%Y-%m-1"))
    end
  end


end
