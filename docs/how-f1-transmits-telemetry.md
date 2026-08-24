# How F1 Actually Transmits Telemetry

Background research for Pitwall. Everything here is sourced; where public figures disagree, the
disagreement is shown rather than averaged away. The last section turns these numbers into the
simulator's default configuration, so `pitwall-source` is calibrated against reality instead of
invented.

---

## 1. The short version

A Formula 1 car is a moving sensor array that cannot send everything it knows. It measures far more
than its radio link can carry, so the system splits into two tiers:

- **A live tier** — a prioritised subset streamed off the car over RF while it is running, sized to a
  hard bandwidth budget, feeding the pit wall and the factory in near real time.
- **A full-fidelity tier** — everything, logged onboard and offloaded over a wired "umbilical" when
  the car stops in the garage.

That split is the single most important thing to understand, and it is the same split that shows up in
every observability pipeline: a sampled hot path for decisions now, a complete cold path for analysis
later. Pitwall models the hot path.

---

## 2. The numbers

### Sensors per car

| Figure | Source |
|---|---|
| 300 sensors | [Yahoo Sports](https://sports.yahoo.com/articles/much-data-does-f1-car-035100099.html) |
| over 250 sensors | [f1briefing](https://f1briefing.com/how-f1-sensors-collect-data-in-real-time/) |
| 1,000–2,000 *channels* per car at 100–1,000 Hz | [F1 Chronicle](https://f1chronicle.com/f1-telemetry-and-data-explained/) |

**Sensors and channels are not the same thing.** One physical sensor can produce several derived
channels, and the ECU synthesises many more. "300 sensors" and "1,500 channels" are both true and
describe different layers. Pitwall models *channels*, because a channel is what actually becomes a
time series.

### Sample rates

Channels are not sampled uniformly. Published ranges put them between **100 Hz and 1,000 Hz**
([F1 Chronicle](https://f1chronicle.com/f1-telemetry-and-data-explained/)). In practice the rate
tracks how fast the physical quantity changes:

- Slow-moving thermal channels (tyre carcass temperature, oil temperature) — order 1–10 Hz
- Vehicle dynamics (speed, throttle, brake pressure, steering angle) — order 100 Hz
- Fast structural and combustion channels (vibration, knock, damper travel) — up to 1 kHz+

This non-uniformity matters for Pitwall: a realistic generator emits **different sensors at different
rates**, which is what makes the key space high-cardinality and the arrival pattern uneven. A generator
where every sensor ticks at the same frequency produces an unrealistically well-behaved stream.

### Aggregate throughput

| Figure | Scope | Source |
|---|---|---|
| 1.1 million data points/second | whole grid | [Yahoo Sports](https://sports.yahoo.com/articles/much-data-does-f1-car-035100099.html) |
| 150,000 data points/second | per car (250+ sensors) | [f1briefing](https://f1briefing.com/how-f1-sensors-collect-data-in-real-time/) |

These two are hard to reconcile — 20 cars at 150k/s would be 3M/s, not 1.1M/s. The likely explanation
is that the 1.1M figure counts the **transmitted** live subset across the grid while the 150k figure
counts what a single car **measures** onboard. That reading is consistent with the two-tier model and
with the bandwidth budget below. Treat 1.1M/s as the *wire* number and 150k/car as the *sensor* number.

### Volume

| Figure | Scope | Source |
|---|---|---|
| ~30 MB per lap | live telemetry, per car | [Yahoo Sports](https://sports.yahoo.com/articles/much-data-does-f1-car-035100099.html) |
| 2–3× that again per pit visit | umbilical offload, per car | [Yahoo Sports](https://sports.yahoo.com/articles/much-data-does-f1-car-035100099.html) |
| over 1.5 TB per race weekend | per car | [Yahoo Sports](https://sports.yahoo.com/articles/much-data-does-f1-car-035100099.html) |
| ~160 TB per race weekend | whole event, all teams, incl. video | [Yahoo Sports](https://sports.yahoo.com/articles/much-data-does-f1-car-035100099.html) |
| a petabyte weekend is approaching | forward projection | [Forbes](https://www.forbes.com/sites/johnkoetsier/2026/05/23/formula-1s-data-explosion-the-petabyte-race-weekend-is-not-far-off/) |

The commonly repeated "F1 generates terabytes per race" is right, but the number you quote depends
entirely on scope. **Per car, per weekend: ~1.5 TB.** Whole event including broadcast video: ~160 TB.

The 30 MB/lap figure is the interesting one for an engineer, because it is a *constraint*, not a
statistic. A 90-second lap at 30 MB is roughly **2.7 Mbit/s of sustained telemetry per car**. That is a
small pipe for 1,500 channels — which is exactly why the live tier is a prioritised subset and why the
wire format is binary and tightly packed rather than JSON.

---

## 3. How it physically moves

### Car to trackside

Formula 1 Management operates its own **WiMAX (802.16) mesh around each circuit** in a licensed band
around **3.5 GHz**, with overlapping access points so a car crossing between cells at 300+ km/h hands
over without dropping the link ([F1 Chronicle](https://f1chronicle.com/f1-telemetry-and-data-explained/)).
The car transmits from an antenna at the front; trackside antennas receive
([f1-fansite](https://www.f1-fansite.com/glossary/telemetry/)).

Handover between cells, tunnels, and pit-lane structures are why **dropout is normal, not exceptional**.
The onboard logger keeps recording through a dropout and the missing window is reconciled afterwards —
which is precisely Pitwall's dropout-then-replay scenario, and the reason late and duplicate data has
to be a first-class design concern rather than an error case.

### Telemetry is one-way

Since the FIA banned two-way telemetry in 2003, data flows **car → pit only**. Teams cannot send
settings back to the car mid-session; they can only tell the driver what to change over the radio
([f1-fansite](https://www.f1-fansite.com/glossary/telemetry/)).

### The standard ECU and ATLAS

Since 2008 every team runs the same **McLaren Applied standard ECU (SECU)**, which is what lets the FIA
enforce complete, comparable logging across the grid. Data is decoded in the garage and distributed to
engineers over Ethernet through **ATLAS** (Advanced Telemetry Linked Acquisition System), also McLaren
Applied — the industry-standard analysis client every team uses
([formula1-dictionary](https://www.formula1-dictionary.net/telemetry.html),
[f1briefing](https://f1briefing.com/how-wireless-telemetry-works-in-f1/)).

Each team's stream is **end-to-end encrypted** and private; only the FIA holds access to designated
compliance channels ([F1 Chronicle](https://f1chronicle.com/f1-telemetry-and-data-explained/)).

### Trackside to factory

Every modern team mirrors the trackside feed to a **Race Operations Centre / Race Support Room** at the
home factory over a leased link, where a larger group of engineers runs parallel simulations against
the live data and flags things the trackside crew cannot see from a snapshot view
([f1-fansite](https://www.f1-fansite.com/glossary/telemetry/),
[Mercedes-AMG PETRONAS](https://www.mercedesamgf1.com/news/feature-data-and-electronics-in-f1-explained)).

This is the **two-audience problem** in its purest form: the pit wall needs the freshest possible value
of a handful of channels, and the factory needs to run heavy historical comparisons over everything.
One store cannot serve both well. Pitwall's split between a live push path and a TimescaleDB query path
is a direct model of this.

---

## 4. The pipeline, end to end

```
  ~1,500 channels/car @ 1–1000 Hz
            │
            ▼
   onboard sensors + standard ECU
            │
            ├──────────────► onboard logger  ──(wired umbilical, in garage)──► full-fidelity archive
            │                                                                   2–3× the live volume
            ▼
   prioritised live subset  ~30 MB/lap  ≈ 2.7 Mbit/s per car
            │
            ▼
   WiMAX 802.16 mesh @ ~3.5 GHz, encrypted, one-way, cell handover at 300 km/h
            │
            ▼
   trackside decode ──► ATLAS on the pit wall (live traces, alerts)
            │
            └──► leased link ──► factory Race Operations Centre (simulation, historical comparison)
```

---

## 5. What Pitwall takes from this

Six properties of the real system that the simulator and pipeline should reproduce:

| Real-world property | How Pitwall models it |
|---|---|
| A prioritised live subset, not everything | `pitwall-source` emits the live tier only; the full-fidelity tier is out of scope |
| Non-uniform sample rates per channel | Each sensor definition carries its own Hz, not one global rate |
| High cardinality — cars × channels | Key space is `(carId, sensorId)`; hot-partition risk is real |
| Dropout is normal (cells, tunnels, pit structures) | Toggleable dropout-then-replay mode producing duplicates and late data |
| Hard bandwidth budget on the wire | Binary wire format (Protobuf) is justified by the 2.7 Mbit/s/car constraint, not by taste |
| Two audiences, two read patterns | Live SSE push (pit wall) + TimescaleDB historical query API (factory) |

### Derived default configuration

Every channel in the catalogue carries its own sample rate, taken from the ranges above — 1–2 Hz for
oil and water temperature, 5 Hz for tyre temperatures and pressures, 100–200 Hz for vehicle dynamics,
500–1000 Hz for damper travel and chassis vibration. There is **one** load dial, `rate-scale`, which
multiplies every channel's rate at once, so turning it up preserves the non-uniform shape of the stream
instead of flattening it.

```yaml
pitwall:
  source:
    cars: 20
    sensors-per-car: 100
    rate-scale: 1.0
```

Three named Spring profiles:

| Profile | Cars × channels × scale | Target events/sec | Purpose |
|---|---|---|---|
| `dev` | 5 × 20 × 0.1 | 870 | Runs anywhere, fast feedback loop |
| `race` | 20 × 100 × 1.0 | 232,600 | The realistic target — the number the demo is built around |
| `breakit` | 20 × 300 × 4.0 | 2,702,400 | Well past the real grid's wire volume; used to find where each stage fails |

`race` at 232,600 events/second sits in the same order of magnitude as the real grid's ~1.1M
points/second live wire figure, from 20 cars running 100 of their ~1,500 channels. `breakit`
deliberately exceeds the real number: the point of the project is not to match F1, it is to find the
ceiling of each architectural choice, so the top of the dial has to be past the realistic operating
point.

### Measured on the development machine

| Profile | Target events/sec | Sustained events/sec | Notes |
|---|---|---|---|
| `race` | 232,600 | ~225,000 | 97% of target, held steady |
| `race` + burst ×3 | 697,800 | ~644,000 | 8-second burst, auto-reverts |
| `breakit` | 2,702,400 | ~1,120,000 | Generator itself is the bottleneck |

The generator reports target and actual separately and never tries to catch up on missed ticks. When
the machine cannot keep up it simply runs slower and the gap between the two numbers shows it — which
is the honest behaviour, and the first place a load story can lie if you let it.

## Sources

- [How Much Data Does An F1 Car Generate? — Yahoo Sports](https://sports.yahoo.com/articles/much-data-does-f1-car-035100099.html)
- [F1 Telemetry and Data: What Teams See in Real Time — F1 Chronicle](https://f1chronicle.com/f1-telemetry-and-data-explained/)
- [How F1 Sensors Collect Data in Real Time — f1briefing](https://f1briefing.com/how-f1-sensors-collect-data-in-real-time/)
- [F1 Telemetry: How Wireless Data Drives Race Performance — f1briefing](https://f1briefing.com/how-wireless-telemetry-works-in-f1/)
- [Telemetry in F1: one-way data, Race Operations Centre and rules — f1-fansite](https://www.f1-fansite.com/glossary/telemetry/)
- [F1 Telemetry: Real-Time Data Systems — Formula 1 Dictionary](https://www.formula1-dictionary.net/telemetry.html)
- [Feature: Data and Electronics in F1, Explained! — Mercedes-AMG PETRONAS F1 Team](https://www.mercedesamgf1.com/news/feature-data-and-electronics-in-f1-explained)
- [Formula 1's Data Explosion: The Petabyte Race Weekend Is Not Far Off — Forbes](https://www.forbes.com/sites/johnkoetsier/2026/05/23/formula-1s-data-explosion-the-petabyte-race-weekend-is-not-far-off/)
