output "instance_id" { value = aws_instance.source.id }
output "instance_public_dns" { value = aws_instance.source.public_dns }
output "instance_public_ip" { value = aws_instance.source.public_ip }
output "landing_bucket" { value = data.aws_s3_bucket.landing.id }
output "landing_uri" { value = "s3://${data.aws_s3_bucket.landing.id}/g3/0-ai-trust/landing/raw" }
output "region" { value = var.region }
output "credit_alert_topic_arn" { value = aws_sns_topic.credit_alerts.arn }
