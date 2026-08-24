create materialized view telemetry_rollup_1m
with (timescaledb.continuous) as
select car_id,
       sensor_id,
       time_bucket(interval '1 minute', event_time) as bucket,
       count(*)   as samples,
       avg(value) as average,
       min(value) as minimum,
       max(value) as maximum
from telemetry_event
group by car_id, sensor_id, bucket
with no data;

create materialized view telemetry_rollup_1h
with (timescaledb.continuous) as
select car_id,
       sensor_id,
       time_bucket(interval '1 hour', bucket)  as bucket,
       sum(samples)                            as samples,
       sum(average * samples) / sum(samples)   as average,
       min(minimum)                            as minimum,
       max(maximum)                            as maximum
from telemetry_rollup_1m
group by car_id, sensor_id, time_bucket(interval '1 hour', bucket)
with no data;

select add_continuous_aggregate_policy('telemetry_rollup_1m',
    start_offset     => interval '7 days',
    end_offset       => interval '1 minute',
    schedule_interval => interval '1 minute');

select add_continuous_aggregate_policy('telemetry_rollup_1h',
    start_offset     => interval '7 days',
    end_offset       => interval '1 hour',
    schedule_interval => interval '5 minutes');

call add_columnstore_policy('telemetry_event', after => interval '1 hour');

select add_retention_policy('telemetry_event', drop_after => interval '7 days');
