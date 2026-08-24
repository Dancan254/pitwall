truncate telemetry_event;

insert into telemetry_event (car_id, sensor_id, event_time, value, sequence_no)
select
    'CAR-' || lpad(car.n::text, 2, '0'),
    sensor.name,
    date_trunc('hour', now()) - interval '1 hour' + (tick.n * interval '30 milliseconds'),
    sensor.floor + (sensor.ceiling - sensor.floor)
        * (0.5 + 0.45 * sin(tick.n / 3000.0 + car.n)),
    tick.n
from generate_series(1, 5) as car(n)
cross join generate_series(0, 95999) as tick(n)
cross join (values
    ('speed', 0.0, 360.0),
    ('engine-rpm', 4000.0, 15000.0),
    ('throttle-position', 0.0, 100.0),
    ('brake-pressure', 0.0, 140.0),
    ('steering-angle', -360.0, 360.0),
    ('gear', 1.0, 8.0),
    ('lateral-acceleration', -6.0, 6.0),
    ('longitudinal-acceleration', -6.0, 6.0),
    ('vertical-acceleration', -8.0, 8.0),
    ('tyre-temperature-front-left', 60.0, 130.0),
    ('tyre-temperature-front-right', 60.0, 130.0),
    ('tyre-temperature-rear-left', 60.0, 130.0),
    ('tyre-temperature-rear-right', 60.0, 130.0),
    ('tyre-pressure-front-left', 18.0, 26.0),
    ('tyre-pressure-front-right', 18.0, 26.0),
    ('brake-temperature-front-left', 200.0, 1000.0),
    ('brake-temperature-front-right', 200.0, 1000.0),
    ('brake-temperature-rear-left', 200.0, 1000.0),
    ('brake-temperature-rear-right', 200.0, 1000.0),
    ('damper-travel-front-left', -30.0, 30.0),
    ('damper-travel-front-right', -30.0, 30.0),
    ('oil-temperature', 80.0, 140.0),
    ('water-temperature', 70.0, 120.0),
    ('fuel-flow', 0.0, 100.0),
    ('energy-store-charge', 0.0, 100.0)
) as sensor(name, floor, ceiling);

analyze telemetry_event;
