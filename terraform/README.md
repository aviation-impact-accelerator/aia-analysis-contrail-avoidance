# AIA Documentation Site Preview: Terraform and AWS

[Terraform](https://developer.hashicorp.com/terraform) is used to describe [AWS](https://aws.amazon.com/) resources allocated to provide a preview of the AIA documentation site when making a pull request on GitHub. This page aims to provide a user guide to Terraform and to document infrastructure design decisions.

## Getting Started

### Prerequisites

1. conda or miniconda
1. conda-lock

### Installation

Working with Terraform in this repository, the following tools need to be installed:

1. [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
1. [TFLint](https://github.com/terraform-linters/tflint#installation)
1. [terraform-docs](https://terraform-docs.io/user-guide/installation/)

These dependencies have been included in the `conda` environment with dev dependencies.

To install run the following command from the root path of the repository:

```bash
conda-lock install --dev-dependencies -n <ENVIRONMENT_NAME> --file conda-lock.yml
```

Verify that Terraform has been installed:

```bash
terraform -help
```

Verify that `TFLint` has been installed:

```bash
tflint --help
```

Verify that `terraform-docs` has been installed:

```bash
terraform-docs --help
```

### Installing pre-commit

Terraform code quality checks have been integrated into the workflow with [`pre-commit`](https://pre-commit.com/). To install Git hook scripts, run the following commands:

```bash
pre-commit install
pre-commit install pre-push
```

The code quality checks will now run whenever code is committed or pushed using Git. Try it out with:

```bash
pre-commit run --all-files
```

You should see the list of checks that run.

### Initializing Terraform

Before any Terraform commands can be run, the backend needs to be initialized and state files can be synced. Run the following command from the `terraform/` directory:

```bash
terraform init
```

This downloads the provider specified in `main.tf`. In this case, our provider is AWS. You will also notice other supporting files created in the `.terraform/` directory. These are `terraform.tfstate` and other provider-specific files. As described in the [state file](#state-file) section, we are using an `S3` backend and the local state file is only a pointer to the remote backend.

### Plan

The command `terraform plan` creates an execution plan based on the configuration files to preview the changes Terraform plans to make to our infrastructure. Read the [documentation](https://developer.hashicorp.com/terraform/cli/commands/plan) for more information. To execute the command, run the following command in the `terraform/` directory:

```bash
terraform plan [options]
```

Input variables need to be provided to the plan in the `[options]` argument. See the [variables](#variables) section for more information.

For more information about other options for the `apply` command, visit the [website](https://developer.hashicorp.com/terraform/cli/commands/apply).

### Apply

The `terraform apply` command executes the actions proposed in a Terraform plan. To execute the command, run the following command in the `terraform/` directory:

```bash
terraform apply [options]
```

Input variables need to be provided to the `apply` command in the `[options]` argument. See the [variables](#variables) section for more information.

Unless the `-auto-approve` option is provided, the user will be prompted to confirm execution in the command shell.

For more information about other options for the `apply` command, visit the [website](https://developer.hashicorp.com/terraform/cli/commands/apply).

### Destroy

To execute the command, run the following command in the `terraform/` directory:

```bash
terraform destroy [options]
```

The `destroy` command does not use input variables but will still require users to include them in the request. This is a known issue and is documented in [`Issue #23552`](https://github.com/hashicorp/terraform/issues/23552). Any input variables are accepted as long as they conform to the variable type.

Unless the `-auto-approve` option is provided, the user will be prompted to confirm execution in the command shell.

For more information about other options for the `apply` command, visit the [website](https://developer.hashicorp.com/terraform/cli/commands/apply).

### Variables

Variables can be provided in-line with `terraform __` commands with the `-var` option. For example:

```bash
terraform apply -var="image_id=ami-abc123"
```

Read the [documentation](https://developer.hashicorp.com/terraform/language/values/variables) for more information.

## Terraform Configuration

### Backend

#### State File

Terraform is used not only to create a stack but it can keep track of the resources allocated in a [state file](https://developer.hashicorp.com/terraform/language/state) and this is used to update the resources if there are changes or to destroy all the resources made with the configuration. Terraform state is saved in a file named `terraform.tfstate`. By default, this state file is saved locally under the `.terraform/` directory when running the `terraform init` command. However, when working with a team, it is useful to have a shared [remote state](https://developer.hashicorp.com/terraform/language/state/remote) that everyone can work from to ensure that everyone has up-to-date information on the infrastructure state. This is achieved with an [S3 backend](https://developer.hashicorp.com/terraform/language/state/remote-state-data).

For simplicity, a single S3 bucket has been set up for the entire organization and each repository has a unique backend with its specific key named after the repository. Unfortunately, the backend configuration cannot [use local variables](https://developer.hashicorp.com/terraform/language/settings/backends/configuration#using-a-backend-block) so the backend configuration needs to be changed for each repository.

#### Locking

Terraform automatically locks the state for operations that could write state. To make changes to the state, the lock must be acquired first. If multiple users are making changes, they will have to wait for the user with the lock to unlock before further changes can be made. Refer to the [documentation](https://developer.hashicorp.com/terraform/language/state/locking) for more information.

When using AWS in the backend, state locking can be done using a DynamoDB table.

!!! warning

    State locking has not been implemented yet. When access to the AIA AWS account is granted, state locking can be implemented. See https://github.com/aviation-impact-accelerator/aia-template-python/issues/43.

### Workspaces

Terraform [workspaces](https://developer.hashicorp.com/terraform/language/state/workspaces) are a useful way to use the same backend and resource stack with a unique configuration for each workspace.

Workspaces are used in this repository to create identical resource stacks in AWS for each pull request. Each stack can be managed individually to update or destroy resources independently of other workspaces.

Workspaces are created when a pull request is first made and named after the pull request number. When the pull request is closed (whether closed manually or merged), the workspace is deleted.

### Style

Terraform provides a [style guide](https://developer.hashicorp.com/terraform/language/style). This should be followed for any changes to the `terraform/` directory.

Given the simplicity of the AIA documentation site, the `terraform/` directory holds all configuration files for the entire resource stack. In the future as complexity increases and the number of managed resources increases, it may be important to refactor the configuration into Terraform modules in other repositories.

## AWS Resources

Resources used:

1. S3 bucket
1. CloudFront

### `S3` Bucket

The AIA documentation site is built into a `site/` directory with all necessary `HTML`, `CSS`, and `JS` files for a cohesive website. There is no server computation needed so an [`S3` bucket](https://aws.amazon.com/s3/) is the simplest and most cost-effective method of hosting the website.

There are various documented methods for hosting a website in an `S3` bucket and they generally trade off simplicity for security.

1. The simplest [solution](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteHosting.html) involves configuring the `S3` bucket as a website endpoint and allowing all read-only traffic from the public (`GET` requests). With this configuration, there are very few security roadblocks between the bucket contents and the public. One notable vulnerability is the lack of DDoS attack protection and the account will get billed directly for all `GET` requests. However, AWS manages the routing of paths to `index.html` files in the subdirectory paths so the bucket acts like a website out of the box _e.g._:
   - `www.example.com/` -> `www.example.com/index.html`
   - `www.example.com/path/to/subdirectory/` -> `www.example.com/path/to/subdirectory/index.html`
1. The preferred (and chosen) solution involves abstracting connections to the bucket using a `CloudFront` content delivery network (CDN). `S3` is set up to only allow `GET` requests from `CloudFront` and any necessary security roadblocks can be attached to `CloudFront`. On top of that, `CloudFront` offers basic DDoS protection automatically. However, subdirectory routing does not work out of the box and either `CloudFront` handles routing or `viewer-requests` are intercepted and the `index.html` route is appended to the `URI`.

#### Terraform Configuration

**Bucket Name**

The name is left blank to allow AWS to choose a random name with Terraform's template prefix. `S3` buckets are globally unique so will need a unique bucket name. To avoid errors caused by bucket names already in use, a random one is generated for us. Since users are never intended to interact with the bucket directly, the name does not have to be concise or consist of words.

**Bucket Policy**

The `S3` bucket policy defines the permissions for accessing the bucket contents. They describe who (`Principal`) can do what (`Effect`) to what (`Resource`) when (`Condition`).

The bucket policy for the chosen configuration allows the `CloudFront` distribution to read objects (`GET`) in the bucket authenticated with an `Origin Access Control (OAC)`.

**Force Destroy**

Typically, an `S3` bucket must be empty before it can be deleted. This feature prevents accidental deletion of the bucket and any data that it holds. In our case, the data in the bucket is intended as temporary files so accidental deletion is not a concern.

The `force_destroy` feature is set to `True` to allow Terraform to destroy the `S3` bucket even if it is not empty.

### `CloudFront`

AWS [`CloudFront`](https://aws.amazon.com/cloudfront/) is a content delivery network (CDN) service that allows secure, high-speed content delivery to users globally. One of the primary purposes of a CDN is to cache data closer to end-users at edge locations to improve speed and reduce networking costs to the origin. This might be caching large video files closer to users so the video file does not need to be transferred unnecessarily between the origin and the edge location more than once.

For our use case, the caching feature is not particularly important as the files served in the documentation site are small and performance speed is not critical. However, `CloudFront` also offers additional security features like basic `DDoS` attack protection and `HTTPS` encryption of data in transit.

#### Terraform Configuration

**Cache Behaviour**

For our use case, caching is not important because file sizes are small and long cache periods mean that changes in the origin (`S3` bucket) take longer to propagate to the end user. For previewing changes in a pull request, rapid propagation is necessary to support rapid development and review.

The cache Time To Live (TTL) is set to 0 so caches expire immediately.

**Default Root Object**

For a Single Page App (SPA), the `index.html` is served at the root of the domain. So requests to the root path `/` will get redirected to `/index.html`. This behavior is only for the root path and does not support routing to other subdirectories.

**Price Class**

AWS has a global network of edge locations that can serve content to end users and by default `Price Class All` serves `CloudFront` data to all edge locations. Development in the AIA is geographically focussed in the United Kingdom so `Price Class 100` is the most cost-efficient option that only serves data to the United States, Mexico, Canada, Europe, Israel, and Türkiye. See [`CloudFront` pricing](https://aws.amazon.com/cloudfront/pricing/) for more information.

**`Origin Access Control`**

`Origin Access Control (OAC)` allows `S3` buckets to securely communicate with permitted `CloudFront` distributions. `OAC` supersedes `OAI` and is now the security best practice. See this [blog article](https://aws.amazon.com/blogs/networking-and-content-delivery/amazon-cloudfront-introduces-origin-access-control-oac/) for more information.

**Geographic Restrictions**

Following the principle of least privilege, AIA development is geographically limited to the United Kingdom for now. So a geographic whitelist is enforced to only allow requests that originate from the UK. See this [guide](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/georestrictions.html) for more information.

**Routing to `index.html`**

`CloudFront` requests to a `S3` bucket are not compatible with `S3`'s website hosting feature. Using `S3`'s website hosting feature, routing subdirectory paths to `index.html` is done if the website endpoint is used. `S3` buckets with website hosting enabled have two endpoints available: (a) bucket endpoint and (b) bucket website endpoint. `CloudFront` recognizes the bucket endpoint as an `S3` origin so the connection can be authenticated with `Origin Access Control` or `Origin Access Identity`. However, `CloudFront` does not recognize the bucket website endpoint as an `S3` bucket is a custom origin and `OAC` or `OAI` are not available. Therefore, the `S3` bucket needs to allow all anonymous public access which negates the security benefits of `CloudFront` since the bucket is public. See this [forum post](https://repost.aws/questions/QUbw5Wr0uuTmafRzC0auUMQA/cloudfront-s3-endpoint-works-but-not-as-website-endpoint) for more information.

To preserve the security benefits of a private `S3` bucket and `CloudFront` distribution, the website hosting feature is disabled and routing is done with `CloudFront` functions. This solution is outlined in this [blog post](https://aws.amazon.com/blogs/networking-and-content-delivery/implementing-default-directory-indexes-in-amazon-s3-backed-amazon-cloudfront-origins-using-cloudfront-functions/). An older [method](https://aws.amazon.com/blogs/compute/implementing-default-directory-indexes-in-amazon-s3-backed-amazon-cloudfront-origins-using-lambdaedge/) uses `Lambda@Edge` instead. See this [blog post](https://aws.amazon.com/blogs/aws/introducing-cloudfront-functions-run-your-code-at-the-edge-with-low-latency-at-any-scale/) for more information about `CloudFront Functions` and comparisons with `Lambda@Edge`.

### Tags

Tags are added to the resource to group resources and to monitor costs relating to specific tags. This is currently configured to tag the repository creating the documentation, the pull request, and the environment (production, development _etc._).

## Code Quality

Terraform code quality is maintained with continuous integration (CI) tools to check:

1. Formatting (per [style guide](https://developer.hashicorp.com/terraform/language/style))
1. Linting
1. Configuration validity
1. Security practice
1. Automatic documentation

### Formatting

The `terraform fmt` command is used to rewrite the Terraform configuration file to a canonical format and style. See [Command `fmt`](https://developer.hashicorp.com/terraform/cli/commands/fmt) for more information.

### Linting

[`TFLint`](https://github.com/terraform-linters/tflint) is an open-source linter and static analysis tool designed to identify potential issues, errors, and violations of best practices in the Terraform configuration. To read more about the rulesets, check out this [repository](https://github.com/terraform-linters/tflint-ruleset-terraform/blob/main/docs/configuration.md).

The `TFLint` configuration is declared in a plugin block in `.tflint.hcl`.

### Configuration Validity

The command `terraform validate` checks and verifies that the configuration is syntactically valid and internally consistent. Read the [documentation](https://developer.hashicorp.com/terraform/cli/commands/validate) for more information.

### Security Practice

Security best practices are followed to the best of our knowledge and based on extensive research.

!!! warning

    No tools are currently configured to run static analyses of security best practices. It is recommended to add [`trivy`](https://github.com/aquasecurity/trivy) to the [CI workflow](https://github.com/antonbabenko/pre-commit-terraform#available-hooks) to validate security practices.

### Automatic Documentation

The reference documentation of the Terraform configuration is found in `terraform/reference.md`. This file outlines the providers, resources, inputs, and outputs and is automatically generated with `terraform-docs`. This file is automatically generated during the pre-commit CI workflow.
