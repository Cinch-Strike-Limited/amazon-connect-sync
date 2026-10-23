# connect_sync: keep a test Amazon Connect instance in line with production

Built and shared by **Cinch + Strike Limited**.

A wizard that runs in **AWS CloudShell** and brings an existing Amazon Connect instance (for example UAT) in line with another instance (for example production). It lists your instances, shows a plan of every change, backs up the target, and only then applies the changes. A restore option undoes a sync.

Provided **as-is** under the [MIT licence](LICENSE), with no warranty and no support obligation. Read the plan before applying, and try it on a non-production instance first.

## Assumptions

- **One way: production to UAT.** The tool refreshes a test instance from production. It is not a deployment tool: changes to production go through your change control process, never through this tool.
- **The target is in a separate AWS account** from production. That's the recommended setup: UAT can't touch production resources, and same-named Lambdas, roles and tables can exist in both accounts. The wizard warns you if both instances are in the same account.

## What it syncs

| Item | Missing in the target | Already in the target (same name) |
|---|---|---|
| Hours of operation | Created | Time zone, hours and description updated |
| Queues | Created | Hours, max contacts, status, description, outbound caller name and outbound flow updated. **The caller ID number is never changed** |
| Flows and flow modules | Created | Content updated. Unpublished drafts in the source are skipped |
| Quick connects (queue and phone number) | Created | Configuration updated |
| Routing profiles | Created | Concurrency, default outbound queue, and queues (added, or priority and delay updated) |
| Security profiles | Created | **Permissions overwritten**; every added or removed permission is listed in the plan and report |
| Custom agent statuses | Created | State and description updated |
| Prompts | Created, if you give an S3 bucket for the audio | Not compared |
| Lambda functions (optional, see below) | **Copy:** created with their own execution role, plus the DynamoDB table structures and EventBridge rules they use. **Link:** same-named functions already in the target are linked to the instance | **Copy:** code and settings updated; the current code is first saved as a published version. Environment variables are kept (they hold the target's own settings); missing keys are added |

**Never changed:** users, phone numbers, queue caller ID numbers, instance settings. **Never deleted:** anything that exists only in the target is left alone and listed in the report.

**Phone numbers inside flows and quick connects** (transfers, staff mobiles) are replaced with a test number you choose, so the target can't ring a real person. Numbers used only to match the dialled number in a flow's conditions are kept, so routing branches still work.

**The source is read-only.** The tool refuses any API call on the source except Get, List, Describe and Search.

## Lambda functions

The wizard offers three choices for the Lambda functions the flows use:

1. **Leave them** (default).
2. **Link** same-named functions that already exist in the target account.
3. **Copy** them from the source account. Only for a target in a separate account.
   - Each new function gets its own execution role (path `/connect-sync/`), with the source role's inline policies (account IDs swapped) and its AWS-managed policies. Customer-managed policies are listed for you to recreate. You can give a permissions boundary.
   - **Secret-looking environment variables** (names containing key, secret, token, password and so on) are set to `<set-me>`; fill them in by hand. Other values are copied, and the plan lists every one so you can check it points at the target's systems.
   - **DynamoDB tables** named in the functions' settings or policies are created with the same structure. **Data is never copied.**
   - **EventBridge rules** that invoke a copied function are copied, with Connect event patterns pointed at the target instance.
   - Runtimes that can no longer be created (for example Node.js 14) are moved to the nearest supported one, and the plan says so. VPC settings and container-image functions aren't copied.

## Before you start

- **Target account:** sign in to the AWS console in the account that holds the target instance, with permissions to manage Amazon Connect (and S3 or Lambda if you use those options).
- **Source in another account:** a role there with the AWS managed policy [`AmazonConnectReadOnlyAccess`](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AmazonConnectReadOnlyAccess.html), which your CloudShell identity can assume. To **copy Lambdas**, that role also needs read access to Lambda (`AWSLambda_ReadOnlyAccess`), IAM roles, DynamoDB (`DescribeTable`, `ListTables`) and EventBridge (`AmazonEventBridgeReadOnlyAccess`).
- **Target permissions for copying Lambdas:** creating IAM roles, Lambda functions and layers, DynamoDB tables and EventBridge rules.
- **A test phone number** in +E.164 format, such as a number claimed on the target instance.
- **Optional:** an S3 bucket in the target account for copying prompt audio, and for keeping a durable copy of the backups.

## Set up the source role (source in another account)

Do this once, in the **source** (production) account. It creates a read-only role that the target account can assume.

1. Sign in to the **source** account and open **CloudShell**.
2. Run this, replacing `<TARGET_ACCOUNT_ID>` with the 12-digit ID of the account that holds the target instance:
   ```bash
   aws iam create-role --role-name ConnectSyncReadOnly --max-session-duration 3600 \
     --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::<TARGET_ACCOUNT_ID>:root"},"Action":"sts:AssumeRole"}]}' \
     --query Role.Arn --output text
   aws iam attach-role-policy --role-name ConnectSyncReadOnly --policy-arn arn:aws:iam::aws:policy/AmazonConnectReadOnlyAccess
   ```
3. **Only if you will use Lambda copy mode,** also attach:
   ```bash
   for p in AWSLambda_ReadOnlyAccess IAMReadOnlyAccess AmazonDynamoDBReadOnlyAccess AmazonEventBridgeReadOnlyAccess; do
     aws iam attach-role-policy --role-name ConnectSyncReadOnly --policy-arn arn:aws:iam::aws:policy/$p
   done
   ```
4. Note the role ARN the first command printed (`arn:aws:iam::<SOURCE_ACCOUNT_ID>:role/ConnectSyncReadOnly`). The wizard asks for it.

Your security team may prefer a narrower trust than the whole target account: for example, only the target account's Identity Center administrator role, or an `sts:ExternalId` condition (the wizard asks for an external ID). Remove the role when you no longer need it:
```bash
for p in $(aws iam list-attached-role-policies --role-name ConnectSyncReadOnly --query 'AttachedPolicies[].PolicyArn' --output text); do
  aws iam detach-role-policy --role-name ConnectSyncReadOnly --policy-arn $p
done
aws iam delete-role --role-name ConnectSyncReadOnly
```

## Run it

1. Open the AWS console in the target account and region, then open **CloudShell** (the `>_` icon in the top bar).
2. Download the tool and run it:
   ```bash
   curl -sSLO https://raw.githubusercontent.com/Cinch-Strike-Limited/amazon-connect-sync/main/connect_sync.py
   python3 connect_sync.py
   ```
   No internet access from CloudShell? Download `connect_sync.py` from this repo, then use **Actions → Upload file** in CloudShell.
4. Choose **Sync**, then follow the prompts: source location (for another account, give the role ARN from the step above), source instance, target instance, test number and options.
5. Read the plan. To apply it, type the **target alias** when asked. Anything else cancels, and nothing changes.

## Backups and restore

- Before applying, the tool saves the target's current configuration to `~/connect-sync-backups/<alias>-before-sync-<time>/backup.json`, and offers to copy it to S3. CloudShell storage is removed after 120 days without use, so keep the S3 copy for anything important.
- The report (`report.md`) is saved in the same folder.
- To undo a sync, run the tool again and choose **Restore**. Restore puts same-named items back as they were. It also saves the current state first, so a restore can itself be undone. Items created by the sync are left in place.

## Good to know

- Re-running a sync is safe: a second run against an unchanged source plans no changes.
- Flows that call Lambda functions or Lex bots need those to exist in the target and be linked to the instance. Use the Lambda copy or link option; Lex bots must be set up in the target separately.
- Restore covers the Connect configuration. To roll back a copied Lambda, use the published version saved before the update.
- Flow names must match between instances. A flow renamed in only one instance is treated as a different flow.
- The Amazon Connect APIs are rate-limited, so a large instance can take a few minutes to read.

## AWS references

- [Migrate flows to an instance, Region, or environment](https://docs.aws.amazon.com/connect/latest/adminguide/migrate-contact-flows.html): the API approach this tool follows.
- [Import and export flows](https://docs.aws.amazon.com/connect/latest/adminguide/contact-flow-import-export.html): the manual, one-flow-at-a-time alternative.

## About

Built by **Cinch + Strike Limited**, Salesforce and Amazon Connect consultants. Issues and suggestions are welcome via GitHub issues.
