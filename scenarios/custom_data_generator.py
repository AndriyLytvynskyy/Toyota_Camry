import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer


def send(producer, topic, key, value, delay=0.5):
    producer.send(topic, key=key.encode(), value=value)
    producer.flush()
    if delay > 0:
        time.sleep(delay)

def scenario_metrics_reference(producer, base):
    """
    EXPECTED FINAL METRICS (exact):
      clicksReceived      = 2
      pageViewsReceived   = 2
      pageViewsEmitted    = 2
      pageViewsUpdated    = 2
      clickStateSize      = 2
      pageViewStateSize   = 2
    """

    user = "user_1"

    # Page view arrives first → emitted with NULL attribution
    send(producer, "page_views", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=10)).isoformat(),
        "url": "https://example.com/ref",
        "event_id": "pv_REF_1",
    })

    #First click (valid but not latest yet)
    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=3)).isoformat(),
        "campaign_id": "campaign_A",
        "click_id": "click_REF_1",
    })
    #
    # Second click (better → triggers update)
    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=5)).isoformat(),
        "campaign_id": "campaign_B",
        "click_id": "click_REF_2",
    })

    # # Extra page view to keep pageViewStateSize = 2
    send(producer, "page_views", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=12)).isoformat(),
        "url": "https://example.com/ref2",
        "event_id": "pv_REF_2",
    })


def scenario_two_clicks_then_pageview(producer, base):
    """
    EXPECTED:
      clicksReceived += 2
      pageViewsReceived += 1
      pageViewsEmitted += 1
      pageViewsUpdated += 0
      clickStateSize -> eventually 2 (then evicted later)
      pageViewStateSize -> 1
    """
    user = "user_1"
    scenario = "S1"

    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=0)).isoformat(),
        "campaign_id": "campaign_A",
        "click_id": "click_S1_1",
    })

    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=5)).isoformat(),
        "campaign_id": "campaign_B",
        "click_id": "click_S1_2",
    })

    send(producer, "page_views", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=10)).isoformat(),
        "url": "https://example.com/p1",
        "event_id": "pv_S1_1",
    })



def scenario_pageview_then_two_clicks(producer, base):
    """
    EXPECTED:
      pageViewsEmitted += 1 (initial null attribution)
      pageViewsUpdated += 2 (each better click updates)
    """
    user = "user_1"
    scenario = "S2"

    send(producer, "page_views", user, {
        "scenario": scenario,
        "user_id": user,
        "event_time": (base + timedelta(minutes=10)).isoformat(),
        "url": "https://example.com/p2",
        "event_id": "pv_S2_1",
    })

    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": base.isoformat(),
        "campaign_id": "campaign_A",
        "click_id": "click_S2_1",
    })

    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=5)).isoformat(),
        "campaign_id": "campaign_B",
        "click_id": "click_S2_2",
    })


def scenario_late_click_dropped(producer, base):
    """
    EXPECTED:
      clicksReceived += 2
      clicksDroppedLate += 1
      pageViewsEmitted += 1
      attribution = click_new
    """
    user = "user_1"
    scenario = "S3"

    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=10)).isoformat(),
        "campaign_id": "campaign_NEW",
        "click_id": "click_S3_new",
    })

    send(producer, "ad_clicks", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=3)).isoformat(),
        "campaign_id": "campaign_OLD",
        "click_id": "click_S3_old",
    })

    send(producer, "page_views", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=11)).isoformat(),
        "url": "https://example.com/p3",
        "event_id": "pv_S3_1",
    })


def scenario_late_pageview_dropped(producer, base):
    """
    EXPECTED:
      pageViewsReceived += 2
      pageViewsDroppedLate += 1
      pageViewsEmitted += 1
    """
    user = "user_1"
    scenario = "S4"

    send(producer, "page_views", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=20)).isoformat(),
        "url": "https://example.com/advance",
        "event_id": "pv_S4_advance",
    })

    send(producer, "page_views", user, {
        "user_id": user,
        "event_time": (base + timedelta(minutes=5)).isoformat(),
        "url": "https://example.com/late",
        "event_id": "pv_S4_late",
    })


SCENARIOS = {
    "two_clicks_then_pageview": scenario_two_clicks_then_pageview,
    "pageview_then_two_clicks": scenario_pageview_then_two_clicks,
    "late_click_dropped": scenario_late_click_dropped,
    "late_pageview_dropped": scenario_late_pageview_dropped,
    "metrics_reference": scenario_metrics_reference,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=SCENARIOS.keys())
    parser.add_argument("--bootstrap", default="kafka:29092")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    base = datetime(2026, 1, 1, 12, 0, 0)

    print(f"\n▶ Running scenario: {args.scenario}")
    print("▶ Watch the UI metrics now...\n")

    SCENARIOS[args.scenario](producer, base)

    producer.close()
    print("\n✔ Scenario finished\n")


if __name__ == "__main__":
    main()
