<!-- BEGIN_TF_DOCS -->

## Requirements

| Name                                                                     | Version  |
| ------------------------------------------------------------------------ | -------- |
| <a name="requirement_terraform"></a> [terraform](#requirement_terraform) | >= 1.2.0 |
| <a name="requirement_aws"></a> [aws](#requirement_aws)                   | ~> 4.16  |

## Providers

| Name                                             | Version |
| ------------------------------------------------ | ------- |
| <a name="provider_aws"></a> [aws](#provider_aws) | 4.67.0  |

## Modules

No modules.

## Resources

| Name                                                                                                                                                           | Type        |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| [aws_cloudfront_distribution.docs_site](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudfront_distribution)                   | resource    |
| [aws_cloudfront_origin_access_control.docs_site](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudfront_origin_access_control) | resource    |
| [aws_s3_bucket.docs_site](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket)                                               | resource    |
| [aws_s3_bucket_policy.allow_access_from_cloudfront_oac](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_policy)          | resource    |
| [aws_iam_policy_document.allow_access_from_cloudfront_oac](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |

## Inputs

| Name                                                                  | Description                                                                                         | Type     | Default | Required |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | -------- | ------- | :------: |
| <a name="input_pull_request"></a> [pull_request](#input_pull_request) | Pull request number that triggered the preview. Resources allocated are unique to the pull request. | `number` | n/a     |   yes    |

## Outputs

| Name                                                                          | Description                                 |
| ----------------------------------------------------------------------------- | ------------------------------------------- |
| <a name="output_s3_bucket_name"></a> [s3_bucket_name](#output_s3_bucket_name) | Globally unique S3 bucket name.             |
| <a name="output_site_url"></a> [site_url](#output_site_url)                   | Domain name of the CloudFront distribution. |

<!-- END_TF_DOCS -->
