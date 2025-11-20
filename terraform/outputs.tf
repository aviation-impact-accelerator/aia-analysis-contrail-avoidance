output "s3_bucket_name" {
  value       = aws_s3_bucket.docs_site.bucket
  description = "Globally unique S3 bucket name."
}

output "site_url" {
  value       = aws_cloudfront_distribution.docs_site.domain_name
  description = "Domain name of the CloudFront distribution."
}
