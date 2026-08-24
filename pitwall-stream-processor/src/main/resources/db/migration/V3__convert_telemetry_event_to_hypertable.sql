create extension if not exists timescaledb;

select create_hypertable(
    'telemetry_event',
    by_range('event_time', interval '5 minutes'),
    migrate_data => true
);

alter table telemetry_event set (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby          = 'car_id, sensor_id',
    timescaledb.orderby            = 'event_time desc'
);
