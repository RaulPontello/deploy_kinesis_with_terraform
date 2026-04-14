# ─────────────────────────────────────────────
#  SNS Topic – Stock Spike Alerts
# ─────────────────────────────────────────────

resource "aws_sns_topic" "spike_alerts" {
  name = "${local.generic_prefix}-spike-alerts"
}

# Email subscription — AWS sends a confirmation email on first apply.
# The subscriber must click "Confirm subscription" before alerts are delivered.
resource "aws_sns_topic_subscription" "alert_email" {
  topic_arn = aws_sns_topic.spike_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
