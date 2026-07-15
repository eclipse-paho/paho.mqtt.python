# Comparaison smoke core — courant vs origin/master

Même harness (branch `benchmarks` @ `42fc4cd`), même profil smoke, broker local identique.

| Version | Commit Paho | Artefact |
|---|---|---|
| **Courant** (branch benchmarks + WIP `client.py`) | `42fc4cd` + WIP reconnect replay | `core-smoke-42fc4cd-wip-20260712T061127Z` |
| **origin/master** | `7a3d161` | `core-smoke-origin-master-7a3d161-20260712T061127Z` |

> Δ% = (courant / master) − 1. Smoke = bruit élevé ; signes seulement si écarts gros et stables.
> Les points `not_implemented:*` sont refusés explicitement (pas de faux chiffres).

## Débits principaux

| Scénario / point | master msg/s | courant msg/s | Δ% | master | courant |
|---|---:|---:|---:|---|---|
| application_rtt_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.25 | 252 | 251 | -0.4% | valid | valid |
| application_rtt_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.5 | 504 | 499 | -1.0% | valid | valid |
| application_rtt_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.75 | 746 | 751 | +0.8% | valid | valid |
| application_rtt_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.9 | 906 | 905 | -0.1% | valid | valid |
| burst_recovery · payload=telemetry256 · qos_publish=0 · cadence=burst · inflight=20 · subscription=hash · topic_topology=fleet4k_uniform | 5,000 | 5,000 | -0.0% | valid | valid |
| duplex_gateway · payload=telemetry256 · qos_publish=0 · cadence=burst · inflight=20 · subscription=exact · topic_topology=single | 848 | 2,013 | +137.5% | valid | valid |
| duplex_gateway · payload=telemetry256 · qos_publish=0 · cadence=steady50 · inflight=20 · subscription=exact · topic_topology=single | 1,000 | 1,000 | -0.0% | valid | valid |
| e2e_integrity · payload=empty0 · qos_publish=0 · cadence=steady50 · inflight=20 · subscription=exact · topic_topology=single | 1,000 | 1,000 | +0.0% | valid | valid |
| e2e_integrity · payload=telemetry256 · qos_publish=0 · cadence=steady50 · inflight=20 · subscription=exact · topic_topology=single | 1,000 | 1,000 | +0.0% | valid | valid |
| e2e_integrity · payload=telemetry256 · qos_publish=1 · cadence=steady50 · inflight=20 · subscription=exact · topic_topology=single | 1,000 | 1,000 | -0.0% | valid | valid |
| e2e_integrity · payload=telemetry256 · qos_publish=2 · cadence=steady50 · inflight=20 · subscription=exact · topic_topology=single | 1,000 | 1,000 | -0.0% | valid | valid |
| pub_payload_sweep_qos0 · payload=binary64 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 11,852 | 29,129 | +145.8% | valid | valid |
| pub_payload_sweep_qos0 · payload=blob1m · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 618 | 659 | +6.7% | valid | valid |
| pub_payload_sweep_qos0 · payload=block64k · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 8,138 | 8,628 | +6.0% | valid | valid |
| pub_payload_sweep_qos0 · payload=empty0 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 12,634 | 29,703 | +135.1% | valid | valid |
| pub_payload_sweep_qos0 · payload=event1k · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 13,486 | 30,276 | +124.5% | valid | valid |
| pub_payload_sweep_qos0 · payload=record16k · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 11,028 | 20,516 | +86.0% | valid | valid |
| pub_payload_sweep_qos0 · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 11,958 | 27,857 | +133.0% | valid | valid |
| pub_qos1_inflight · payload=telemetry256 · qos_publish=1 · cadence=capacity · inflight=1 · subscription=exact · topic_topology=single | 4,512 | 5,603 | +24.2% | valid | valid |
| pub_qos1_inflight · payload=telemetry256 · qos_publish=1 · cadence=capacity · inflight=100 · subscription=exact · topic_topology=single | 9,064 | 20,896 | +130.5% | valid | valid |
| pub_qos1_inflight · payload=telemetry256 · qos_publish=1 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 9,496 | 15,489 | +63.1% | valid | valid |
| pub_qos_sweep_telemetry · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 12,031 | 27,635 | +129.7% | valid | valid |
| pub_qos_sweep_telemetry · payload=telemetry256 · qos_publish=1 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 8,755 | 16,480 | +88.2% | valid | valid |
| pub_qos_sweep_telemetry · payload=telemetry256 · qos_publish=2 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 7,081 | 11,308 | +59.7% | valid | valid |
| puback_latency_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.25 | 250 | 250 | +0.0% | valid | valid |
| puback_latency_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.5 | 500 | 500 | +0.1% | valid | valid |
| puback_latency_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.75 | 750 | 750 | -0.0% | valid | valid |
| puback_latency_qos1 · payload=telemetry256 · qos_publish=1 · cadence=loaded75 · inflight=20 · subscription=exact · topic_topology=single · load_fraction=0.9 | 900 | 900 | +0.0% | valid | valid |
| remaining_length_boundaries · payload=rl_126 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 13,032 | 29,471 | +126.1% | valid | valid |
| remaining_length_boundaries · payload=rl_127 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 12,551 | 29,131 | +132.1% | valid | valid |
| remaining_length_boundaries · payload=rl_128 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 12,068 | 27,847 | +130.7% | valid | valid |
| remaining_length_boundaries · payload=rl_16383 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 10,878 | 21,180 | +94.7% | valid | valid |
| remaining_length_boundaries · payload=rl_16384 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 11,796 | 21,182 | +79.6% | valid | valid |
| sub_callback_matching · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=hash · topic_topology=fleet4k_uniform · callback_filters=1 | 5,300 | 5,331 | +0.6% | valid | valid |
| sub_callback_matching · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=hash · topic_topology=fleet4k_uniform · callback_filters=16 | 5,310 | 5,279 | -0.6% | valid | valid |
| sub_callback_matching · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=hash · topic_topology=fleet4k_uniform · callback_filters=256 | 5,013 | 5,039 | +0.5% | valid | valid |
| sub_exact_telemetry · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=exact · topic_topology=single | 5,382 | 5,265 | -2.2% | valid | valid |
| sub_hierarchy_telemetry · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=hash · topic_topology=fleet4k_uniform | 5,199 | 5,330 | +2.5% | valid | valid |
| sub_hierarchy_telemetry · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=hash · topic_topology=fleet4k_zipf | — | — | — | inconclusive | inconclusive |
| sub_hierarchy_telemetry · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=plus · topic_topology=fleet4k_uniform | 5,295 | 5,357 | +1.2% | valid | valid |
| sub_hierarchy_telemetry · payload=telemetry256 · qos_publish=0 · cadence=capacity · inflight=20 · subscription=plus · topic_topology=fleet4k_zipf | — | — | — | inconclusive | inconclusive |

## Notes

- Harness post-correctifs : `blob1m` non nul, `sub_callback_matching` compte les deliveries filtrées, integrity exacte, duplex réel.
- Variantes Zipf refusées (`not_implemented:topic_topology:fleet4k_zipf`) des deux côtés.
- WIP non commité côté courant : replay CONNACK `loop_write` unique (voir `client.py` dirty).
