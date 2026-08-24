create table telemetry_event (
    id          bigserial primary key,
    car_id      text             not null,
    sensor_id   text             not null,
    event_time  timestamptz      not null,
    value       double precision not null,
    sequence_no bigint           not null
);

create index idx_telemetry_event_car_time on telemetry_event (car_id, event_time desc);
