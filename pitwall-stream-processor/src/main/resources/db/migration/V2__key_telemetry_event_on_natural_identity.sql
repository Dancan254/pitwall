delete from telemetry_event duplicate
using telemetry_event original
where duplicate.id > original.id
  and duplicate.car_id = original.car_id
  and duplicate.sensor_id = original.sensor_id
  and duplicate.event_time = original.event_time;

alter table telemetry_event drop column id;

alter table telemetry_event
    add constraint pk_telemetry_event primary key (car_id, sensor_id, event_time);
