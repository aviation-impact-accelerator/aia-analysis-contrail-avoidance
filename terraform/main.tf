terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
  backend "s3" {
    bucket = "aviation-impact-accelerator-terraform-state"
    key    = "aia_model_contrial_avoidance/terraform.tfstate"
    region = "eu-west-2"
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "eu-west-2"
}

resource "aws_s3_bucket" "docs_site" {
  tags = {
    Repository  = local.repository_name
    Environment = "Dev"
    PullRequest = var.pull_request
  }
  force_destroy = true
}

resource "aws_cloudfront_distribution" "docs_site" {
  provider            = aws
  enabled             = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100"
  origin {
    origin_id                = "${aws_s3_bucket.docs_site.bucket}-origin"
    domain_name              = aws_s3_bucket.docs_site.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.docs_site.id
  }

  default_cache_behavior {

    target_origin_id = "${aws_s3_bucket.docs_site.bucket}-origin"
    allowed_methods  = ["GET", "HEAD"]
    cached_methods   = ["GET", "HEAD"]

    forwarded_values {
      query_string = true

      cookies {
        forward = "all"
      }
    }

    function_association {
      event_type   = "viewer-request"
      function_arn = local.cloudfront_function_servedefaulthtmlobject_arn
    }

    viewer_protocol_policy = "redirect-to-https"
    min_ttl                = 0
    default_ttl            = 0
    max_ttl                = 0
  }

  restrictions {
    geo_restriction {
      restriction_type = "whitelist"
      locations        = ["GB"]
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "aws_cloudfront_origin_access_control" "docs_site" {
  name                              = "docs-${local.repository_name}-#${var.pull_request}"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

data "aws_iam_policy_document" "allow_access_from_cloudfront_oac" {
  statement {
    sid       = "AllowCloudFrontServicePrincipalReadOnly"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.docs_site.arn}/*"]
    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.docs_site.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "allow_access_from_cloudfront_oac" {
  bucket = aws_s3_bucket.docs_site.bucket
  policy = data.aws_iam_policy_document.allow_access_from_cloudfront_oac.json
}
