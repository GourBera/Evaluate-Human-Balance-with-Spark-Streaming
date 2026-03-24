import json
import os
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, Response
from kafka import KafkaConsumer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CUSTOMER_RISK", "customer-risk")
GRAPH_PORT = int(os.getenv("GRAPH_PORT", "8080"))


app = Flask(__name__)


state_lock = threading.Lock()
current_window = None
# Store latest score for each customer inside current minute window.
window_scores = {}


def minute_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M")


def parse_message(raw: bytes):
    try:
        payload = json.loads(raw.decode("utf-8"))
        birth_year = payload.get("birthYear")
        customer = payload.get("customer") or payload.get("email")
        score = float(payload.get("score"))
        if not birth_year or not customer:
            return None
        return {
            "customer": customer,
            "birthYear": str(birth_year),
            "score": score,
        }
    except Exception:
        return None


def consume_customer_risk():
    global current_window

    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=None,
                consumer_timeout_ms=1000,
            )

            for message in consumer:
                parsed = parse_message(message.value)
                if not parsed:
                    continue

                now = datetime.now(timezone.utc)
                win = minute_key(now)

                with state_lock:
                    if current_window != win:
                        current_window = win
                        window_scores.clear()
                    key = f"{parsed['birthYear']}::{parsed['customer']}"
                    window_scores[key] = parsed

            consumer.close()
        except Exception as exc:
            print(f"[graph] consumer error: {exc}", flush=True)
            time.sleep(2)


def aggregate_by_birth_year():
    # Return individual data points as {x: birthYear, y: score} for scatter plot.
    with state_lock:
        points = []
        for row in window_scores.values():
            points.append({"x": int(row["birthYear"]), "y": row["score"]})

        return {
            "window": current_window,
            "points": points,
            "count": len(window_scores),
        }


@app.route("/")
def index():
    return Response(
        """
<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>STEDI Risk Graph</title>
    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>
    <style>
      body { font-family: Arial, sans-serif; background: #f2f2f2; margin: 0; }
      .wrap { max-width: 1200px; margin: 20px auto; padding: 0 16px; }
      h2 { text-align: center; color: #444; margin-bottom: 4px; }
      .sub { text-align: center; color: #666; margin-bottom: 12px; }
      .meta { text-align: center; color: #666; margin-bottom: 12px; }
      .card { background: white; border: 1px solid #ddd; padding: 16px; }
      canvas { width: 100%; height: 560px; }
    </style>
  </head>
  <body>
    <div class=\"wrap\">
      <h2>STEDI Population Risk Change by Birth Year (below zero is deterioration, greater than zero is improvement)</h2>
      <div class=\"sub\">Spark Result Set (since we see data for every customer every minute, the graph reads every minute)</div>
      <div id=\"meta\" class=\"meta\">Loading...</div>
      <div class=\"card\">
        <canvas id=\"riskChart\"></canvas>
      </div>
    </div>

    <script>
      const ctx = document.getElementById('riskChart');
      const meta = document.getElementById('meta');

      const chart = new Chart(ctx, {
        type: 'scatter',
        data: {
          datasets: [{
            label: 'Spark Result Set (since we see data for every customer every minute, the graph reads every minute)',
            data: [],
            backgroundColor: '#e96a82',
            borderColor: '#e96a82',
            pointRadius: 5,
            pointHoverRadius: 7
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            y: {
              suggestedMin: -40,
              suggestedMax: 20,
              title: { display: false }
            },
            x: {
              suggestedMin: 1900,
              suggestedMax: 2000,
              title: { display: false }
            }
          }
        }
      });

      async function refreshData() {
        try {
          const res = await fetch('/api/data');
          const data = await res.json();

          chart.data.datasets[0].data = data.points;
          chart.update();

          meta.textContent = `Window: ${data.window || 'n/a'} UTC | Customers in window: ${data.count}`;
        } catch (e) {
          meta.textContent = 'Waiting for stream data...';
        }
      }

      refreshData();
      setInterval(refreshData, 2000);
    </script>
  </body>
</html>
        """,
        mimetype="text/html",
    )


@app.route("/api/data")
def api_data():
    return jsonify(aggregate_by_birth_year())


if __name__ == "__main__":
    t = threading.Thread(target=consume_customer_risk, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=GRAPH_PORT)
