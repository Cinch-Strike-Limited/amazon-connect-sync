#!/usr/bin/env python3
"""
connect_sync.py - bring an existing Amazon Connect instance (for example UAT) in line with
another instance (for example production). An interactive wizard for AWS CloudShell.

Built by Cinch + Strike Limited. MIT licence; provided as-is, with no warranty and no support obligation.
Read the plan it prints, and try it on a non-production instance first.

Intended use: one way, production -> UAT, with UAT in a separate AWS account. It refreshes a test
instance; it is not a deployment tool. Changes to production go through change control.

Run it in CloudShell, signed in to the account that holds the TARGET instance:

    python3 connect_sync.py

What it does
  - Creates what is missing in the target and updates same-named items to match the source:
    hours of operation, queues, flows, flow modules, quick connects, routing profiles,
    security profiles and custom agent statuses (and prompts, if you give it an S3 bucket).
  - Never touches users or phone numbers, and never changes a queue's caller ID number.
  - Leaves items that exist only in the target alone, and lists them in the report.
  - Replaces phone numbers inside flows and quick connects with your test number,
    so the target can never ring a real person.
  - Optionally links the Lambda functions the flows use (it never copies Lambda code).
  - Backs up the target before changing it; the menu can restore that backup.
  - Treats the source as read-only: it refuses any call except Get, List, Describe and Search.
"""
import datetime as dt
import glob
import json
import os
import re
import sys
import time
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, ParamValidationError

VERSION = "1.0.1"
RETRY = Config(retries={"max_attempts": 10, "mode": "adaptive"})
BACKUP_ROOT = os.path.expanduser("~/connect-sync-backups")
PLACEHOLDER = "<new "
READ_PREFIXES = ("Get", "List", "Describe", "Search")
E164 = re.compile(r"^\+[1-9]\d{7,14}$")
ATTRIBUTES = ["INBOUND_CALLS", "OUTBOUND_CALLS", "CONTACTFLOW_LOGS", "CONTACT_LENS", "EARLY_MEDIA",
              "AUTO_RESOLVE_BEST_VOICES", "USE_CUSTOM_TTS_VOICES", "MULTI_PARTY_CONFERENCE",
              "ENHANCED_CONTACT_MONITORING", "ENHANCED_CHAT_MONITORING"]
API_ERRORS = (ClientError, ParamValidationError, BotoCoreError, RuntimeError)

# kind: (list op, list key, describe op, describe key, id argument, name field, id field)
KINDS = {
    "hours": ("list_hours_of_operations", "HoursOfOperationSummaryList", "describe_hours_of_operation",
              "HoursOfOperation", "HoursOfOperationId", "Name", "HoursOfOperationId"),
    "agent_statuses": ("list_agent_statuses", "AgentStatusSummaryList", "describe_agent_status",
                       "AgentStatus", "AgentStatusId", "Name", "AgentStatusId"),
    "security_profiles": ("list_security_profiles", "SecurityProfileSummaryList", "describe_security_profile",
                          "SecurityProfile", "SecurityProfileId", "SecurityProfileName", "Id"),
    "prompts": ("list_prompts", "PromptSummaryList", "describe_prompt", "Prompt", "PromptId", "Name", "PromptId"),
    "queues": ("list_queues", "QueueSummaryList", "describe_queue", "Queue", "QueueId", "Name", "QueueId"),
    "modules": ("list_contact_flow_modules", "ContactFlowModulesSummaryList", "describe_contact_flow_module",
                "ContactFlowModule", "ContactFlowModuleId", "Name", "Id"),
    "flows": ("list_contact_flows", "ContactFlowSummaryList", "describe_contact_flow", "ContactFlow",
              "ContactFlowId", "Name", "Id"),
    "quick_connects": ("list_quick_connects", "QuickConnectSummaryList", "describe_quick_connect", "QuickConnect",
                       "QuickConnectId", "Name", "QuickConnectId"),
    "routing_profiles": ("list_routing_profiles", "RoutingProfileSummaryList", "describe_routing_profile",
                         "RoutingProfile", "RoutingProfileId", "Name", "RoutingProfileId"),
}
LABELS = {"hours": "hours of operation", "agent_statuses": "agent status", "security_profiles": "security profile",
          "prompts": "prompt", "queues": "queue", "modules": "flow module", "flows": "flow",
          "quick_connects": "quick connect", "routing_profiles": "routing profile", "lambdas": "Lambda",
          "tables": "DynamoDB table", "rules": "EventBridge rule"}
SECRETISH = re.compile(r"pass|secret|token|key|credential|private|auth", re.I)
RUNTIME_MAP = {"nodejs12.x": "nodejs18.x", "nodejs14.x": "nodejs18.x", "nodejs16.x": "nodejs18.x",
               "python3.6": "python3.9", "python3.7": "python3.9", "python3.8": "python3.9"}


# -- small helpers ---------------------------------------------------------

def paged(fn, key, **kw):
    items = []
    while True:
        resp = fn(**kw)
        items.extend(resp.get(key, []))
        if not resp.get("NextToken"):
            return items
        kw["NextToken"] = resp["NextToken"]


def err(e):
    if hasattr(e, "response"):
        error = e.response.get("Error", {})
        msg = error.get("Message") or error.get("Code") or str(e)
        problems = e.response.get("problems")
        if problems:
            msg += " | " + "; ".join(p.get("message", str(p)) for p in problems)
        return msg
    return str(e)


def stamp():
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")


def by_name(items):
    return {i["_name"]: i for i in items}


def ask(prompt, default=None, required=False, check=None):
    while True:
        try:
            value = input(f"{prompt}{f' [{default}]' if default else ''}: ").strip()
        except (EOFError, KeyboardInterrupt):
            sys.exit("\nCancelled - nothing was changed.")
        value = value or (default or "")
        if not value and required:
            print("  A value is required.")
            continue
        problem = check(value) if value and check else None
        if problem:
            print(f"  {problem}")
            continue
        return value


def yes(prompt, default=False):
    return ask(f"{prompt} (y/n)", "y" if default else "n").lower().startswith("y")


def choose(prompt, items, label):
    for n, item in enumerate(items, 1):
        print(f"  {n}. {label(item)}")
    pick = ask(prompt, required=True,
               check=lambda v: None if v.isdigit() and 1 <= int(v) <= len(items) else f"Enter 1 to {len(items)}")
    return items[int(pick) - 1]


def make_read_only(client):
    """Refuse every API call on this client except Get/List/Describe/Search."""
    def guard(model, **_):
        if not model.name.startswith(READ_PREFIXES):
            raise RuntimeError(f"refused {model.name}: the source instance is read-only")
    client.meta.events.register(f"before-call.{client.meta.service_model.service_id.hyphenize()}", guard)


# -- AWS access -----------------------------------------------------------

class Account:
    def __init__(self, session):
        self.session = session
        self.region = session.region_name
        self.id = session.client("sts", config=RETRY).get_caller_identity()["Account"]

    def client(self, service, read_only=False):
        c = self.session.client(service, config=RETRY)
        if read_only:
            make_read_only(c)
        return c

    def instances(self):
        c = self.client("connect", read_only=True)
        return [i for i in paged(c.list_instances, "InstanceSummaryList") if i.get("InstanceStatus") == "ACTIVE"]


class Instance:
    def __init__(self, account, summary, read_only=False):
        self.account, self.read_only = account, read_only
        self.c = account.client("connect", read_only)
        self.id, self.arn = summary["Id"], summary["Arn"]
        self.alias = summary.get("InstanceAlias", "")
        self.region = account.region

    def list(self, op, key, **kw):
        return paged(getattr(self.c, op), key, InstanceId=self.id, **kw)

    def meta(self):
        return {"id": self.id, "arn": self.arn, "alias": self.alias, "region": self.region,
                "account": self.account.id}


def snapshot(inst):
    """Read everything the sync compares, from one instance, into a plain dict (also the backup format)."""
    snap = {"tool": f"connect_sync {VERSION}", "taken": dt.datetime.now().isoformat(timespec="seconds"),
            "meta": inst.meta(), "items": {}, "extra": {}}
    print(f"Reading {inst.alias} ...")
    for kind, (lop, lkey, dop, dkey, idarg, namef, idf) in KINDS.items():
        kw = {"QueueTypes": ["STANDARD"]} if kind == "queues" else {}
        out = []
        for s in inst.list(lop, lkey, **kw):
            if "ARCHIVED" in (s.get("ContactFlowState"), s.get("State")):
                continue
            d = getattr(inst.c, dop)(InstanceId=inst.id, **{idarg: s["Id"]})[dkey]
            d["_id"], d["_name"] = d[idf], d[namef]
            if kind == "queues":
                d["_quick_connects"] = [q["Id"] for q in inst.list(
                    "list_queue_quick_connects", "QuickConnectSummaryList", QueueId=d["_id"])]
            elif kind == "routing_profiles":
                d["_queues"] = [{k: q[k] for k in ("QueueId", "Channel", "Priority", "Delay")} for q in inst.list(
                    "list_routing_profile_queues", "RoutingProfileQueueConfigSummaryList", RoutingProfileId=d["_id"])]
            elif kind == "security_profiles":
                d["_permissions"] = sorted(paged(inst.c.list_security_profile_permissions, "Permissions",
                                                 InstanceId=inst.id, SecurityProfileId=d["_id"]))
            out.append(d)
        snap["items"][kind] = out
        print(f"  {LABELS[kind]}: {len(out)}")
    numbers = paged(inst.c.list_phone_numbers_v2, "ListPhoneNumbersSummaryList", TargetArn=inst.arn)
    snap["extra"]["phone_numbers"] = [{k: n[k] for k in ("PhoneNumber", "PhoneNumberId", "PhoneNumberArn")}
                                      for n in numbers]
    snap["extra"]["lambdas"] = inst.list("list_lambda_functions", "LambdaFunctions")
    snap["extra"]["users"] = len(inst.list("list_users", "UserSummaryList"))
    attrs = {}
    for a in ATTRIBUTES:
        try:
            attrs[a] = inst.c.describe_instance_attribute(InstanceId=inst.id, AttributeType=a)["Attribute"]["Value"]
        except API_ERRORS:
            pass
    snap["extra"]["attributes"] = attrs
    return snap


# -- the sync engine ------------------------------------------------------

class Sync:
    """Plans (apply=False) or applies (apply=True) the changes that make `tgt` match `src_snap`.

    The same code runs both ways: a plan run records what it would do; an apply run does it.
    `restore=True` replays a backup of the same instance: no phone-number replacement.
    """

    def __init__(self, src_snap, tgt, tgt_snap, opts, apply=False, restore=False):
        self.s, self.t = src_snap["items"], tgt_snap["items"]
        self.sm, self.tm = src_snap["meta"], tgt_snap["meta"]
        self.src_extra, self.tgt_extra = src_snap["extra"], tgt_snap["extra"]
        self.tgt, self.tc = tgt, tgt.c if tgt else None
        self.opts, self.apply, self.restore = opts, apply, restore
        self.idmap = {}
        self.plan = {k: {"create": [], "update": [], "same": 0, "target_only": []}
                     for k in list(KINDS) + ["tables", "lambdas", "rules"]}
        self.done, self.warnings, self.perm_changes, self.env_notes = [], [], [], []
        self.phone_hits = 0
        self.lex = set()
        self._phones()
        for kind in KINDS:  # same name = same item
            tgt_by = by_name(self.t[kind])
            for s in self.s[kind]:
                if s["_name"] in tgt_by:
                    self.idmap[s["_id"]] = tgt_by[s["_name"]]["_id"]

    # helpers ----------------------------------------------------------

    def warn(self, msg):
        if msg not in self.warnings:
            self.warnings.append(msg)
            if self.apply:
                print(f"  [warn] {msg}")

    def m(self, src_id):
        return self.idmap.get(src_id) if src_id else None

    def act(self, kind, action, name, fn=None, id_key=None, src_id=None, detail=""):
        label = f"{action} {LABELS[kind]} '{name}'" + (f" ({detail})" if detail else "")
        if not self.apply:
            self.plan[kind][action].append(f"{name} ({detail})" if detail else name)
            if action == "create" and src_id:
                self.idmap[src_id] = f"{PLACEHOLDER}{kind}:{name}>"
            return True
        try:
            result = fn()
            if action == "create" and src_id and id_key:
                self.idmap[src_id] = result[id_key]
            self.done.append(label)
            print(f"  [done] {label}")
            return True
        except API_ERRORS as e:
            self.warn(f"could not {label}: {err(e)}")
            return False

    def same(self, kind):
        if not self.apply:
            self.plan[kind]["same"] += 1

    def only_in_target(self, kind):
        src_names = {s["_name"] for s in self.s[kind]}
        if not self.apply:
            self.plan[kind]["target_only"] = sorted(t["_name"] for t in self.t[kind] if t["_name"] not in src_names)

    @staticmethod
    def desc(kw, item):
        if item.get("Description"):
            kw["Description"] = item["Description"]
        return kw

    @staticmethod
    def desc_changed(s, t):
        return bool(s.get("Description")) and s.get("Description") != t.get("Description")

    # phone numbers ------------------------------------------------------

    def _phones(self):
        cc = (self.opts.get("country_code") or "").lstrip("+")
        self.cc = cc
        pattern = r"(?<![\w-])\+[1-9]\d{7,14}(?![\w-])"
        if cc:
            pattern += r"|(?<![\w-])0[2-9]\d{6,9}(?![\w-])"
        self.phone_re = re.compile(pattern)
        self.src_digits = {self.digits(n["PhoneNumber"]) for n in self.src_extra.get("phone_numbers", [])}
        tgt_numbers = self.tgt_extra.get("phone_numbers", [])
        self.tgt_digits = {self.digits(n["PhoneNumber"]) for n in tgt_numbers}
        first = sorted(tgt_numbers, key=lambda n: n["PhoneNumber"])[0] if tgt_numbers else None
        self.tgt_number = first["PhoneNumber"] if first else None
        if first:  # caller IDs on new queues point at the target's own number
            for n in self.src_extra.get("phone_numbers", []):
                self.idmap[n["PhoneNumberId"]] = first["PhoneNumberId"]
                self.idmap[n["PhoneNumberArn"]] = first["PhoneNumberArn"]

    def digits(self, number):
        d = re.sub(r"\D", "", number)
        if self.cc and d.startswith(self.cc) and number.strip().startswith("+"):
            d = d[len(self.cc):]
        return d.lstrip("0")

    def map_phone(self, match):
        raw = match.group(0)
        d = self.digits(raw)
        if d in self.tgt_digits:
            return raw
        if d in self.src_digits and self.tgt_number:
            return self.tgt_number
        self.phone_hits += 1
        return self.opts["test_number"]

    # content remapping --------------------------------------------------

    def remap(self, text, phones=True):
        for a, b in self.idmap.items():
            if a != b:
                text = text.replace(a, b)
        sm, tm = self.sm, self.tm
        text = text.replace(sm["arn"], tm["arn"]).replace(sm["id"], tm["id"])
        if (sm["region"], sm["account"]) != (tm["region"], tm["account"]):
            text = re.sub(rf"(arn:aws[a-z-]*:[a-z0-9-]+:){re.escape(sm['region'])}:{sm['account']}:",
                          rf"\g<1>{tm['region']}:{tm['account']}:", text)
            text = re.sub(rf"(arn:aws[a-z-]*:[a-z0-9-]+::){sm['account']}:", rf"\g<1>{tm['account']}:", text)
        if phones and not self.restore:
            text = self.phone_re.sub(self.map_phone, text)
        return text

    def remap_flow(self, content):
        """remap() for flow and module JSON. Phone numbers used as Compare operands (matched against the
        dialled number, never called) are kept, so routing on the dialled number keeps its branches."""
        doc = json.loads(content)
        keep = {}
        for a in doc.get("Actions", []):
            for cond in (a.get("Transitions", {}).get("Conditions") or []):
                ops = cond.get("Condition", {}).get("Operands", [])
                for i, v in enumerate(ops):
                    if isinstance(v, str) and self.phone_re.fullmatch(v.strip()):
                        token = f"__KEEPNUM{len(keep)}__"
                        keep[token], ops[i] = v, token
        text = self.remap(json.dumps(doc))
        for token, v in keep.items():
            text = text.replace(token, v)
        return text

    # resources, in dependency order -----------------------------------

    def hours(self):
        tgt = by_name(self.t["hours"])
        norm = lambda cfg: sorted(json.dumps(c, sort_keys=True) for c in cfg)  # noqa: E731
        for s in self.s["hours"]:
            t = tgt.get(s["_name"])
            if not t:
                kw = self.desc(dict(InstanceId=self.tm["id"], Name=s["Name"], TimeZone=s["TimeZone"],
                                    Config=s["Config"]), s)
                self.act("hours", "create", s["_name"], lambda kw=kw: self.tc.create_hours_of_operation(**kw),
                         "HoursOfOperationId", s["_id"])
                continue
            changes = [n for n, c in (("time zone", s["TimeZone"] != t["TimeZone"]),
                                      ("hours", norm(s["Config"]) != norm(t["Config"])),
                                      ("description", self.desc_changed(s, t))) if c]
            if not changes:
                self.same("hours")
                continue
            kw = self.desc(dict(InstanceId=self.tm["id"], HoursOfOperationId=t["_id"], Name=t["Name"],
                                TimeZone=s["TimeZone"], Config=s["Config"]), s)
            self.act("hours", "update", s["_name"], lambda kw=kw: self.tc.update_hours_of_operation(**kw),
                     detail=", ".join(changes))
        self.only_in_target("hours")

    def agent_statuses(self):
        tgt = by_name(self.t["agent_statuses"])
        for s in self.s["agent_statuses"]:
            if s.get("Type") != "CUSTOM":
                continue
            t = tgt.get(s["_name"])
            if not t:
                kw = self.desc(dict(InstanceId=self.tm["id"], Name=s["Name"], State=s["State"]), s)
                self.act("agent_statuses", "create", s["_name"], lambda kw=kw: self.tc.create_agent_status(**kw),
                         "AgentStatusId", s["_id"])
            elif s["State"] != t.get("State") or self.desc_changed(s, t):
                kw = self.desc(dict(InstanceId=self.tm["id"], AgentStatusId=t["_id"], State=s["State"]), s)
                self.act("agent_statuses", "update", s["_name"], lambda kw=kw: self.tc.update_agent_status(**kw))
            else:
                self.same("agent_statuses")
        self.only_in_target("agent_statuses")

    def security_profiles(self):
        tgt = by_name(self.t["security_profiles"])
        for s in self.s["security_profiles"]:
            t = tgt.get(s["_name"])
            perms = s["_permissions"]
            if not t:
                kw = self.desc(dict(InstanceId=self.tm["id"], SecurityProfileName=s["_name"], Permissions=perms), s)
                self.act("security_profiles", "create", s["_name"],
                         lambda kw=kw: self.tc.create_security_profile(**kw), "SecurityProfileId", s["_id"])
                continue
            added = sorted(set(perms) - set(t["_permissions"]))
            removed = sorted(set(t["_permissions"]) - set(perms))
            if not (added or removed or self.desc_changed(s, t)):
                self.same("security_profiles")
                continue
            if not self.apply and (added or removed):
                self.perm_changes.append((s["_name"], added, removed))
            kw = self.desc(dict(InstanceId=self.tm["id"], SecurityProfileId=t["_id"], Permissions=perms), s)
            self.act("security_profiles", "update", s["_name"], lambda kw=kw: self.tc.update_security_profile(**kw),
                     detail=f"+{len(added)} / -{len(removed)} permissions")
        self.only_in_target("security_profiles")

    def prompts(self):
        tgt = by_name(self.t["prompts"])
        bucket = self.opts.get("prompt_bucket")
        for s in self.s["prompts"]:
            if s["_name"] in tgt:
                self.same("prompts")  # audio isn't compared
                continue
            if self.restore or not bucket:
                self.warn(f"prompt '{s['_name']}' is missing in the target and wasn't copied "
                          f"({'restore has no audio' if self.restore else 'no S3 bucket given'}); flows using it may fail")
                continue
            self.act("prompts", "create", s["_name"], lambda s=s: self._copy_prompt(s, bucket), "PromptId", s["_id"])
        self.only_in_target("prompts")

    def _copy_prompt(self, s, bucket):
        src_c = self.opts["source_client"]
        url = src_c.get_prompt_file(InstanceId=self.sm["id"], PromptId=s["_id"])["PromptPresignedUrl"]
        ext = re.search(r"\.(wav|mp3|ulaw)(?:\?|$)", url, re.I)
        key = f"connect-sync/prompts/{re.sub(r'[^A-Za-z0-9._-]+', '_', s['_name'])}.{ext.group(1).lower() if ext else 'wav'}"
        with urllib.request.urlopen(url, timeout=60) as r:
            self.opts["target_s3"].put_object(Bucket=bucket, Key=key, Body=r.read())
        kw = self.desc(dict(InstanceId=self.tm["id"], Name=s["_name"], S3Uri=f"s3://{bucket}/{key}"), s)
        return self.tc.create_prompt(**kw)

    def queues(self):
        tgt = by_name(self.t["queues"])
        for s in self.s["queues"]:
            t = tgt.get(s["_name"])
            hours = self.m(s.get("HoursOfOperationId"))
            if not t:
                if not hours:
                    self.warn(f"queue '{s['_name']}' skipped - its hours of operation weren't copied")
                    continue
                kw = self.desc(dict(InstanceId=self.tm["id"], Name=s["Name"], HoursOfOperationId=hours), s)
                if s.get("MaxContacts"):
                    kw["MaxContacts"] = s["MaxContacts"]
                occ = s.get("OutboundCallerConfig") or {}
                out = {}
                if occ.get("OutboundCallerIdName"):
                    out["OutboundCallerIdName"] = occ["OutboundCallerIdName"]
                if occ.get("OutboundCallerIdNumberId") and self.m(occ["OutboundCallerIdNumberId"]):
                    out["OutboundCallerIdNumberId"] = self.m(occ["OutboundCallerIdNumberId"])
                if out:
                    kw["OutboundCallerConfig"] = out
                self.act("queues", "create", s["_name"], lambda kw=kw: self.tc.create_queue(**kw), "QueueId", s["_id"])
                continue
            calls, changes = [], []
            qid, iid = t["_id"], self.tm["id"]
            if hours and hours != t.get("HoursOfOperationId"):
                changes.append("hours")
                calls.append(("update_queue_hours_of_operation", dict(InstanceId=iid, QueueId=qid, HoursOfOperationId=hours)))
            if s.get("MaxContacts") and s["MaxContacts"] != t.get("MaxContacts"):
                changes.append("max contacts")
                calls.append(("update_queue_max_contacts", dict(InstanceId=iid, QueueId=qid, MaxContacts=s["MaxContacts"])))
            if s.get("Status") and s["Status"] != t.get("Status"):
                changes.append("status")
                calls.append(("update_queue_status", dict(InstanceId=iid, QueueId=qid, Status=s["Status"])))
            if self.desc_changed(s, t):
                changes.append("description")
                calls.append(("update_queue_name", dict(InstanceId=iid, QueueId=qid, Name=t["Name"], Description=s["Description"])))
            if not changes:
                self.same("queues")
                continue
            self.act("queues", "update", s["_name"],
                     lambda calls=calls: [getattr(self.tc, op)(**kw) for op, kw in calls], detail=", ".join(changes))
        self.only_in_target("queues")

    def lambda_names(self):
        """Every Lambda the source uses: its instance links plus ARNs inside flow and module content."""
        arns = set(self.src_extra.get("lambdas", []))
        for kind in ("flows", "modules"):
            for s in self.s[kind]:
                arns |= set(re.findall(r'arn:aws[a-z-]*:lambda:[^"\\\s]+', s.get("Content", "")))
        return sorted({a.split(":function:")[1].split(":")[0] for a in arns if ":function:" in a})

    def lambdas(self):
        """Lambda functions the flows use: leave (default), link the target's same-named ones, or copy them."""
        if self.restore:
            return  # a restore replays the same instance; its Lambdas are unchanged
        names = self.lambda_names()
        mode = self.opts.get("lambda_mode", "skip")
        if not names:
            return
        if mode == "skip":
            self.warn(f"{len(names)} Lambda function(s) are used by the source and the Lambda option is off; flows "
                      f"that call a Lambda not already linked to the target may fail to save")
            return
        if mode == "copy":
            LambdaCopier(self).run(names)
        self.link_lambdas(names)

    def link_lambdas(self, names):
        linked = set(self.tgt_extra.get("lambdas", []))
        lam = self.opts["target_lambda"]
        created = {n for n in self.plan["lambdas"]["create"]} if not self.apply else set()
        for name in names:
            ta = f"arn:aws:lambda:{self.tm['region']}:{self.tm['account']}:function:{name}"
            if ta in linked:
                self.same("lambdas")
                continue
            if not any(c.startswith(f"{name} (") for c in created):
                try:
                    lam.get_function_configuration(FunctionName=name)
                except API_ERRORS:
                    self.warn(f"Lambda '{name}' doesn't exist in the target account - deploy or copy it, "
                              f"then run the sync again")
                    continue
            self.act("lambdas", "create", f"link {name}", lambda ta=ta: self.tc.associate_lambda_function(
                InstanceId=self.tm["id"], FunctionArn=ta), detail="to the instance")

    def flows_and_modules(self):
        keep = set(self.opts.get("keep") or [])
        tgt = {k: by_name(self.t[k]) for k in ("flows", "modules")}
        for kind in ("flows", "modules"):
            for s in self.s[kind]:
                self.lex |= set(re.findall(r'arn:aws[a-z-]*:lex:[^"\\\s]+', s.get("Content", "")))
        # 1. create what's missing; flows and modules can reference each other, so retry until no progress
        pending = [(k, s) for k in ("modules", "flows") for s in self.s[k] if s["_name"] not in tgt[k]]
        if not self.apply:
            for k, s in pending:
                self.act(k, "create", s["_name"], src_id=s["_id"], detail=s.get("Type", "") if k == "flows" else "")
        else:
            errors = {}
            while pending:
                progress = False
                for k, s in list(pending):
                    kw = self.desc(dict(InstanceId=self.tm["id"], Name=s["_name"], Content=self.remap_flow(s["Content"])), s)
                    try:
                        if k == "flows":
                            kw["Type"] = s["Type"]
                            if s.get("Status"):
                                kw["Status"] = s["Status"]
                            self.idmap[s["_id"]] = self.tc.create_contact_flow(**kw)["ContactFlowId"]
                        else:
                            self.idmap[s["_id"]] = self.tc.create_contact_flow_module(**kw)["Id"]
                        self.done.append(f"create {LABELS[k]} '{s['_name']}'")
                        print(f"  [done] create {LABELS[k]} '{s['_name']}'")
                        pending.remove((k, s))
                        progress = True
                    except API_ERRORS as e:
                        errors[(k, s["_id"])] = err(e)
                if not progress:
                    break
            for k, s in pending:
                self.warn(f"could not create {LABELS[k]} '{s['_name']}': {errors.get((k, s['_id']))}")
        # 2. bring existing ones in line with the source
        for kind in ("modules", "flows"):
            for s in self.s[kind]:
                t = tgt[kind].get(s["_name"])
                if not t:
                    continue
                if s["_name"] in keep:
                    self.same(kind)
                    continue
                if kind == "flows" and s.get("Type") != t.get("Type"):
                    self.warn(f"flow '{s['_name']}' is a {s.get('Type')} in the source but a {t.get('Type')} "
                              f"in the target - left as it is")
                    continue
                if kind == "flows" and s.get("Status") == "SAVED" and not self.restore:
                    self.same(kind)  # unpublished draft in the source: don't push it into the target
                    continue
                content = self.remap_flow(s["Content"])
                if json.loads(content) == json.loads(t["Content"]) and not self.desc_changed(s, t):
                    self.same(kind)
                    continue

                def update(kind=kind, t=t, content=content, s=s):
                    if kind == "flows":
                        self.tc.update_contact_flow_content(InstanceId=self.tm["id"], ContactFlowId=t["_id"], Content=content)
                        if self.desc_changed(s, t):
                            self.tc.update_contact_flow_name(InstanceId=self.tm["id"], ContactFlowId=t["_id"],
                                                             Name=t["_name"], Description=s["Description"])
                    else:
                        self.tc.update_contact_flow_module_content(InstanceId=self.tm["id"],
                                                                   ContactFlowModuleId=t["_id"], Content=content)
                self.act(kind, "update", s["_name"], update)
        self.only_in_target("flows")
        self.only_in_target("modules")

    def queue_caller_config(self):
        """Outbound caller name and outbound flow; the target's caller ID number is always kept."""
        tgt = by_name(self.t["queues"])
        for s in self.s["queues"]:
            qid = self.m(s["_id"])
            occ = s.get("OutboundCallerConfig") or {}
            if not qid:
                continue
            t = tgt.get(s["_name"])
            current = (t or {}).get("OutboundCallerConfig") or {}
            want = {}
            if occ.get("OutboundCallerIdName"):
                want["OutboundCallerIdName"] = occ["OutboundCallerIdName"]
            number = current.get("OutboundCallerIdNumberId") or (None if t else self.m(occ.get("OutboundCallerIdNumberId")))
            if number:
                want["OutboundCallerIdNumberId"] = number
            if occ.get("OutboundFlowId"):
                flow = self.m(occ["OutboundFlowId"])
                if not flow:
                    self.warn(f"queue '{s['_name']}': its outbound whisper flow wasn't copied - set it manually")
                    continue
                want["OutboundFlowId"] = flow
            if t and (want.get("OutboundCallerIdName") == current.get("OutboundCallerIdName")
                      and want.get("OutboundFlowId") == current.get("OutboundFlowId")):
                continue
            if not t and not occ.get("OutboundFlowId"):
                continue  # a new queue already got its caller name when it was created
            if not want:
                continue
            self.act("queues", "update", s["_name"], lambda qid=qid, want=want: self.tc.update_queue_outbound_caller_config(
                InstanceId=self.tm["id"], QueueId=qid, OutboundCallerConfig=want), detail="outbound caller settings")

    def quick_connects(self):
        tgt = by_name(self.t["quick_connects"])
        for s in self.s["quick_connects"]:
            cfg = s["QuickConnectConfig"]
            qtype = cfg["QuickConnectType"]
            if qtype == "QUEUE":
                queue, flow = self.m(cfg["QueueConfig"]["QueueId"]), self.m(cfg["QueueConfig"]["ContactFlowId"])
                if not (queue and flow):
                    self.warn(f"quick connect '{s['_name']}' skipped - its queue or flow wasn't copied")
                    continue
                want = {"QuickConnectType": qtype, "QueueConfig": {"QueueId": queue, "ContactFlowId": flow}}
            elif qtype == "PHONE_NUMBER":
                want = {"QuickConnectType": qtype, "PhoneConfig": {"PhoneNumber": self.remap(cfg["PhoneConfig"]["PhoneNumber"])}}
            else:
                self.warn(f"quick connect '{s['_name']}' is a {qtype} quick connect - not synced (users aren't synced)")
                continue
            t = tgt.get(s["_name"])
            if not t:
                kw = self.desc(dict(InstanceId=self.tm["id"], Name=s["_name"], QuickConnectConfig=want), s)
                self.act("quick_connects", "create", s["_name"], lambda kw=kw: self.tc.create_quick_connect(**kw),
                         "QuickConnectId", s["_id"])
                continue
            tcfg = t["QuickConnectConfig"]
            if tcfg["QuickConnectType"] != qtype:
                self.warn(f"quick connect '{s['_name']}' is a different type in the target - left as it is")
                continue
            current = {k: v for k, v in tcfg.items() if k in want}
            if current == want and not self.desc_changed(s, t):
                self.same("quick_connects")
                continue

            def update(t=t, want=want, s=s, changed=current != want):
                if changed:
                    self.tc.update_quick_connect_config(InstanceId=self.tm["id"], QuickConnectId=t["_id"],
                                                        QuickConnectConfig=want)
                if self.desc_changed(s, t):
                    self.tc.update_quick_connect_name(InstanceId=self.tm["id"], QuickConnectId=t["_id"],
                                                      Name=t["_name"], Description=s["Description"])
            self.act("quick_connects", "update", s["_name"], update)
        self.only_in_target("quick_connects")

    def queue_quick_connects(self):
        tgt = by_name(self.t["queues"])
        for s in self.s["queues"]:
            qid = self.m(s["_id"])
            if not qid:
                continue
            want = [w for w in (self.m(q) for q in s["_quick_connects"]) if w]
            have = set(tgt[s["_name"]]["_quick_connects"]) if s["_name"] in tgt else set()
            add = [w for w in want if w not in have]
            if add:
                self.act("queues", "update", s["_name"], lambda qid=qid, add=add: self.tc.associate_queue_quick_connects(
                    InstanceId=self.tm["id"], QueueId=qid, QuickConnectIds=add), detail=f"add {len(add)} quick connect(s)")

    def routing_profiles(self):
        tgt = by_name(self.t["routing_profiles"])
        iid = self.tm["id"]
        for s in self.s["routing_profiles"]:
            want = {}
            for q in s["_queues"]:
                qid = self.m(q["QueueId"])
                if qid:
                    want[(qid, q["Channel"])] = (q["Priority"], q["Delay"])
            configs = [{"QueueReference": {"QueueId": q, "Channel": c}, "Priority": p, "Delay": d}
                       for (q, c), (p, d) in want.items()]
            media = [mc for mc in s["MediaConcurrencies"] if mc.get("Concurrency", 0) >= 1]
            t = tgt.get(s["_name"])
            if not t:
                outbound = self.m(s.get("DefaultOutboundQueueId"))
                if not outbound:
                    self.warn(f"routing profile '{s['_name']}' skipped - its default outbound queue wasn't copied")
                    continue
                kw = dict(InstanceId=iid, Name=s["_name"], Description=s.get("Description") or s["_name"],
                          DefaultOutboundQueueId=outbound, MediaConcurrencies=media)

                def create(kw=kw, configs=configs):
                    if configs:
                        kw["QueueConfigs"] = configs[:10]
                    result = self.tc.create_routing_profile(**kw)
                    for i in range(10, len(configs), 10):
                        self.tc.associate_routing_profile_queues(InstanceId=iid, RoutingProfileId=result["RoutingProfileId"],
                                                                 QueueConfigs=configs[i:i + 10])
                    return result
                self.act("routing_profiles", "create", s["_name"], create, "RoutingProfileId", s["_id"])
                continue
            rid = t["_id"]
            have = {(q["QueueId"], q["Channel"]): (q["Priority"], q["Delay"]) for q in t["_queues"]}
            add = [c for c in configs if (c["QueueReference"]["QueueId"], c["QueueReference"]["Channel"]) not in have]
            change = [c for c in configs if have.get((c["QueueReference"]["QueueId"], c["QueueReference"]["Channel"]))
                      not in (None, (c["Priority"], c["Delay"]))]
            norm = lambda ms: sorted(json.dumps(x, sort_keys=True) for x in ms)  # noqa: E731
            calls, changes = [], []
            if norm(media) != norm([mc for mc in t["MediaConcurrencies"] if mc.get("Concurrency", 0) >= 1]):
                changes.append("concurrency")
                calls.append(("update_routing_profile_concurrency", dict(InstanceId=iid, RoutingProfileId=rid, MediaConcurrencies=media)))
            outbound = self.m(s.get("DefaultOutboundQueueId"))
            if outbound and outbound != t.get("DefaultOutboundQueueId"):
                changes.append("default outbound queue")
                calls.append(("update_routing_profile_default_outbound_queue",
                              dict(InstanceId=iid, RoutingProfileId=rid, DefaultOutboundQueueId=outbound)))
            if self.desc_changed(s, t):
                changes.append("description")
                calls.append(("update_routing_profile_name", dict(InstanceId=iid, RoutingProfileId=rid, Name=t["_name"],
                                                                  Description=s["Description"])))
            for i in range(0, len(add), 10):
                calls.append(("associate_routing_profile_queues", dict(InstanceId=iid, RoutingProfileId=rid, QueueConfigs=add[i:i + 10])))
            for i in range(0, len(change), 10):
                calls.append(("update_routing_profile_queues", dict(InstanceId=iid, RoutingProfileId=rid, QueueConfigs=change[i:i + 10])))
            if add:
                changes.append(f"add {len(add)} queue(s)")
            if change:
                changes.append(f"priority/delay of {len(change)} queue(s)")
            extra = [k for k in have if k not in want]
            if extra:
                self.warn(f"routing profile '{s['_name']}' has {len(extra)} queue(s) in the target only - left in place")
            if not changes:
                self.same("routing_profiles")
                continue
            self.act("routing_profiles", "update", s["_name"],
                     lambda calls=calls: [getattr(self.tc, op)(**kw) for op, kw in calls], detail=", ".join(changes))
        self.only_in_target("routing_profiles")

    def run(self):
        self.hours()
        self.agent_statuses()
        self.security_profiles()
        self.prompts()
        self.queues()
        self.lambdas()
        self.flows_and_modules()
        self.queue_caller_config()
        self.quick_connects()
        self.queue_quick_connects()
        self.routing_profiles()
        for arn in sorted(self.lex):
            self.warn(f"Lex bot {arn.split('/')[-1] if '/' in arn else arn} is used by the source - "
                      f"make sure the target has it and it's linked to the instance")
        return self

    def changes(self):
        return sum(len(p["create"]) + len(p["update"]) for p in self.plan.values())


def paged_by(fn, key, out_token, in_token, **kw):
    items = []
    while True:
        resp = fn(**kw)
        items.extend(resp.get(key, []))
        if not resp.get(out_token):
            return items
        kw[in_token] = resp[out_token]


def download(url):
    with urllib.request.urlopen(url, timeout=120) as r:
        return r.read()


class LambdaCopier:
    """Copies the Lambdas a source instance uses into the target account, under the same names.

    New functions get their own execution role. For functions that already exist in the target, code and
    settings are updated but environment variables are KEPT (they hold the target's own endpoints); missing
    keys are added. Secret-looking values are never copied. Before an update, the current code is saved as a
    published version, so it can be rolled back. DynamoDB tables the functions name are created (structure
    only, never items), and EventBridge rules that invoke the functions are copied.
    """

    def __init__(self, sync):
        self.s, o = sync, sync.opts
        self.src_lambda, self.src_iam = o["source_lambda"], o["source_iam"]
        self.src_ddb, self.src_events = o["source_dynamodb"], o["source_events"]
        self.tl, self.iam = o["target_lambda"], o["target_iam"]
        self.tddb, self.tev = o["target_dynamodb"], o["target_events"]
        self.boundary = o.get("boundary")
        self.layers = {}
        self.texts = []  # env values and policies, to find the DynamoDB tables the functions use
        self.tag = {"copied-by": "connect_sync"}

    def run(self, names):
        have = {f["FunctionName"] for f in paged_by(self.tl.list_functions, "Functions", "NextMarker", "Marker")}
        for name in names:
            try:
                f = self.src_lambda.get_function(FunctionName=name)
            except API_ERRORS as e:
                self.s.warn(f"Lambda '{name}' couldn't be read in the source ({err(e)}) - not copied")
                continue
            cfg = f["Configuration"]
            if cfg.get("PackageType") == "Image":
                self.s.warn(f"Lambda '{name}' is a container image function - not copied; deploy it separately")
                continue
            env = cfg.get("Environment") or {}
            if env.get("Error"):
                self.s.warn(f"Lambda '{name}': environment variables are encrypted in the source and couldn't be read")
            self.texts.append(json.dumps(env.get("Variables") or {}))
            if cfg.get("VpcConfig", {}).get("SubnetIds"):
                self.s.warn(f"Lambda '{name}': VPC settings aren't copied - add the target's subnets if it needs them")
            if name in have:
                self.update(name, f)
            else:
                self.create(name, f)
        self.tables()
        self.rules(names)

    # functions ------------------------------------------------------------

    def runtime(self, name, runtime):
        if runtime in RUNTIME_MAP:
            self.s.warn(f"Lambda '{name}': runtime {runtime} can no longer be created - using {RUNTIME_MAP[runtime]}")
        return RUNTIME_MAP.get(runtime, runtime)

    def new_env(self, name, variables):
        env = {}
        for k, v in (variables or {}).items():
            if SECRETISH.search(k):
                env[k] = "<set-me>"
                self.s.env_notes.append((name, k, "looks secret: set to <set-me>, fill it in by hand"))
            else:
                env[k] = self.s.remap(v, phones=False)
                self.s.env_notes.append((name, k, f"= {env[k]}  (copied - check it points at the target's systems)"))
        return env

    def create(self, name, f):
        cfg = f["Configuration"]
        runtime = self.runtime(name, cfg.get("Runtime"))
        env = self.new_env(name, (cfg.get("Environment") or {}).get("Variables"))
        role_policies = self.source_role(cfg["Role"])

        def create():
            layers = [x for x in (self.layer(l["Arn"]) for l in cfg.get("Layers", [])) if x]
            kw = dict(FunctionName=name, Runtime=runtime, Role=self.role(name, role_policies), Handler=cfg["Handler"],
                      Code={"ZipFile": download(f["Code"]["Location"])}, Timeout=cfg.get("Timeout", 8),
                      MemorySize=cfg.get("MemorySize", 128), Architectures=cfg.get("Architectures", ["x86_64"]),
                      Description=(cfg.get("Description") or "")[:256], Tags=dict(self.tag))
            if env:
                kw["Environment"] = {"Variables": env}
            if layers:
                kw["Layers"] = layers
            for _ in range(10):  # a new role takes a few seconds before Lambda can use it
                try:
                    return self.tl.create_function(**kw)
                except ClientError as e:
                    if "assumed" not in str(e) and "role defined" not in str(e):
                        raise
                    time.sleep(5)
            raise RuntimeError("the new execution role never became usable")
        self.s.act("lambdas", "create", name, create, detail=f"{runtime}, own execution role")

    def update(self, name, f):
        src = f["Configuration"]
        tgt = self.tl.get_function_configuration(FunctionName=name)
        changes, cfg = [], {}
        for key, want in (("Runtime", self.runtime(name, src.get("Runtime"))), ("Handler", src.get("Handler")),
                          ("Timeout", src.get("Timeout")), ("MemorySize", src.get("MemorySize"))):
            if want is not None and tgt.get(key) != want:
                cfg[key] = want
        src_layers = [l["Arn"] for l in src.get("Layers", [])]
        tgt_layer_names = sorted(l["Arn"].split(":")[6] for l in tgt.get("Layers", []))
        if sorted(a.split(":")[6] for a in src_layers) != tgt_layer_names:
            cfg["Layers"] = src_layers  # resolved to target ARNs at apply time
        current = (tgt.get("Environment") or {}).get("Variables") or {}
        src_env = (src.get("Environment") or {}).get("Variables") or {}
        added = {k: ("<set-me>" if SECRETISH.search(k) else self.s.remap(v, phones=False))
                 for k, v in src_env.items() if k not in current}
        for k in added:
            self.s.env_notes.append((name, k, "missing in the target: added" +
                                     (" as <set-me>, fill it in by hand" if added[k] == "<set-me>" else f" = {added[k]}")))
        for k in src_env:
            if k in current and not SECRETISH.search(k) and current[k] != self.s.remap(src_env[k], phones=False):
                self.s.env_notes.append((name, k, "differs from the source: kept as set in the target"))
        if added:
            cfg["Environment"] = {"Variables": dict(current, **added)}
        code_changed = src["CodeSha256"] != tgt["CodeSha256"]
        if not code_changed and not cfg:
            self.s.same("lambdas")
            return
        changes = (["code"] if code_changed else []) + [k.lower() for k in cfg]

        def update(cfg=cfg):
            version = self.tl.publish_version(FunctionName=name,
                                              Description=f"connect_sync backup {stamp()}")["Version"]
            self.s.done.append(f"saved Lambda '{name}' current code as version {version}")
            if code_changed:
                self.tl.update_function_code(FunctionName=name, ZipFile=download(f["Code"]["Location"]))
                self.wait(name)
            if cfg:
                if "Layers" in cfg:
                    cfg["Layers"] = [x for x in (self.layer(a) for a in cfg["Layers"]) if x]
                self.tl.update_function_configuration(FunctionName=name, **cfg)
                self.wait(name)
        self.s.act("lambdas", "update", name, update, detail=", ".join(changes) + "; current code kept as a version")

    def wait(self, name):
        for _ in range(60):
            status = self.tl.get_function_configuration(FunctionName=name).get("LastUpdateStatus")
            if status in (None, "Successful"):
                return
            if status == "Failed":
                raise RuntimeError(f"update of '{name}' failed")
            time.sleep(2)

    def layer(self, src_arn):
        """A layer from the source account is copied (an identical version in the target is reused);
        AWS-published and shared layers are used as they are."""
        if src_arn.split(":")[4] != self.s.sm["account"]:
            return src_arn
        if src_arn in self.layers:
            return self.layers[src_arn]
        name = src_arn.split(":")[6]
        v = self.src_lambda.get_layer_version_by_arn(Arn=src_arn)
        try:
            for tv in self.tl.list_layer_versions(LayerName=name).get("LayerVersions", []):
                t = self.tl.get_layer_version(LayerName=name, VersionNumber=tv["Version"])
                if t["Content"]["CodeSha256"] == v["Content"]["CodeSha256"]:
                    self.layers[src_arn] = t["LayerVersionArn"]
                    return t["LayerVersionArn"]
        except API_ERRORS:
            pass
        kw = dict(LayerName=name, Content={"ZipFile": download(v["Content"]["Location"])},
                  Description=(v.get("Description") or "")[:256])
        if v.get("CompatibleRuntimes"):
            kw["CompatibleRuntimes"] = sorted({RUNTIME_MAP.get(r, r) for r in v["CompatibleRuntimes"]})
        arn = self.tl.publish_layer_version(**kw)["LayerVersionArn"]
        self.s.done.append(f"copied layer '{name}'")
        self.layers[src_arn] = arn
        return arn

    # execution roles --------------------------------------------------------

    def source_role(self, role_arn):
        """The source role's inline policies (remapped) and AWS-managed policy ARNs; customer-managed ones are reported."""
        name = role_arn.split("/")[-1]
        inline, managed = {}, []
        try:
            for pn in self.src_iam.list_role_policies(RoleName=name)["PolicyNames"]:
                doc = self.src_iam.get_role_policy(RoleName=name, PolicyName=pn)["PolicyDocument"]
                text = json.dumps(doc)
                self.texts.append(text)
                inline[pn] = self.s.remap(text, phones=False)
            for p in paged(self.src_iam.list_attached_role_policies, "AttachedPolicies", RoleName=name):
                if p["PolicyArn"].startswith("arn:aws:iam::aws:policy/"):
                    managed.append(p["PolicyArn"])
                else:
                    self.s.warn(f"role '{name}': customer-managed policy '{p['PolicyName']}' isn't copied - "
                                f"create it in the target and attach it to the copied role")
        except API_ERRORS as e:
            self.s.warn(f"role '{name}' couldn't be read in the source ({err(e)}) - the copy gets logging permissions only")
        return inline, managed

    def role(self, fn, policies):
        inline, managed = policies
        name = f"{fn[:46]}-sync-role"
        try:
            return self.iam.get_role(RoleName=name)["Role"]["Arn"]
        except API_ERRORS:
            pass
        trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                                                         "Principal": {"Service": "lambda.amazonaws.com"}}]}
        kw = dict(RoleName=name, Path="/connect-sync/", AssumeRolePolicyDocument=json.dumps(trust),
                  Description=f"Execution role for {fn}, created by connect_sync"[:1000],
                  Tags=[{"Key": k, "Value": v} for k, v in self.tag.items()])
        if self.boundary:
            kw["PermissionsBoundary"] = self.boundary
        arn = self.iam.create_role(**kw)["Role"]["Arn"]
        region, account = self.s.tm["region"], self.s.tm["account"]
        logs = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": f"arn:aws:logs:{region}:{account}:log-group:/aws/lambda/{fn}:*"}]}
        self.iam.put_role_policy(RoleName=name, PolicyName="logging", PolicyDocument=json.dumps(logs))
        for pn, doc in inline.items():
            self.iam.put_role_policy(RoleName=name, PolicyName=pn, PolicyDocument=doc)
        for p in managed:
            self.iam.attach_role_policy(RoleName=name, PolicyArn=p)
        self.s.done.append(f"created role '{name}'")
        return arn

    # DynamoDB tables and EventBridge rules ---------------------------------

    def tables(self):
        """Tables named in the copied functions' settings or policies: created in the target, structure only."""
        try:
            src = paged_by(self.src_ddb.list_tables, "TableNames", "LastEvaluatedTableName", "ExclusiveStartTableName")
            have = set(paged_by(self.tddb.list_tables, "TableNames", "LastEvaluatedTableName", "ExclusiveStartTableName"))
        except API_ERRORS as e:
            self.s.warn(f"DynamoDB tables couldn't be listed ({err(e)}) - none copied")
            return
        text = "\n".join(self.texts)
        for name in src:
            if not re.search(rf"(?<![\w.-]){re.escape(name)}(?![\w.-])", text):
                continue
            if name in have:
                self.s.same("tables")
                continue
            t = self.src_ddb.describe_table(TableName=name)["Table"]
            used = {k["AttributeName"] for k in t["KeySchema"]}
            kw = dict(TableName=name, KeySchema=t["KeySchema"], BillingMode="PAY_PER_REQUEST",
                      Tags=[{"Key": k, "Value": v} for k, v in self.tag.items()])
            for key, src_key in (("GlobalSecondaryIndexes", "GlobalSecondaryIndexes"),
                                 ("LocalSecondaryIndexes", "LocalSecondaryIndexes")):
                idx = [{"IndexName": g["IndexName"], "KeySchema": g["KeySchema"], "Projection": g["Projection"]}
                       for g in t.get(src_key, [])]
                for g in idx:
                    used |= {k["AttributeName"] for k in g["KeySchema"]}
                if idx:
                    kw[key] = idx
            kw["AttributeDefinitions"] = [a for a in t["AttributeDefinitions"] if a["AttributeName"] in used]
            self.s.act("tables", "create", name, lambda kw=kw: self.tddb.create_table(**kw),
                       detail="structure only, no items")

    def rules(self, names):
        """EventBridge rules (default bus) that invoke a copied function: created in the target, pattern remapped."""
        try:
            src_rules = paged(self.src_events.list_rules, "Rules")
            have = {r["Name"] for r in paged(self.tev.list_rules, "Rules")}
        except API_ERRORS as e:
            self.s.warn(f"EventBridge rules couldn't be listed ({err(e)}) - none copied")
            return
        region, account = self.s.tm["region"], self.s.tm["account"]
        for r in src_rules:
            targets = self.src_events.list_targets_by_rule(Rule=r["Name"])["Targets"]
            fns = [t["Arn"].split(":function:")[1].split(":")[0] for t in targets if ":function:" in t["Arn"]]
            fns = [f for f in fns if f in names]
            if not fns:
                continue
            if r["Name"] in have:
                self.s.same("rules")
                continue
            if len(fns) != len(targets):
                self.s.warn(f"rule '{r['Name']}': only its Lambda targets are copied")

            def create(r=r, fns=fns):
                kw = dict(Name=r["Name"], State=r.get("State", "ENABLED"), Description=(r.get("Description") or "")[:512],
                          Tags=[{"Key": k, "Value": v} for k, v in self.tag.items()])
                if r.get("EventPattern"):
                    kw["EventPattern"] = self.s.remap(r["EventPattern"], phones=False)
                if r.get("ScheduleExpression"):
                    kw["ScheduleExpression"] = r["ScheduleExpression"]
                rule_arn = self.tev.put_rule(**kw)["RuleArn"]
                self.tev.put_targets(Rule=r["Name"], Targets=[
                    {"Id": f"t{i}", "Arn": f"arn:aws:lambda:{region}:{account}:function:{f}"} for i, f in enumerate(fns)])
                for f in fns:
                    try:
                        self.tl.add_permission(FunctionName=f, StatementId=f"events-{r['Name']}"[:100],
                                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                                               SourceArn=rule_arn)
                    except ClientError as e:
                        if "ResourceConflict" not in str(e):
                            raise
            self.s.act("rules", "create", r["Name"], create, detail=f"invokes {', '.join(fns)}")


# -- reporting -----------------------------------------------------------

def print_plan(sync, title):
    print(f"\n{title}")
    print(f"  {'Item':<20}{'Create':>8}{'Update':>8}{'Same':>8}{'Target only':>13}")
    for kind, p in sync.plan.items():
        if any((p["create"], p["update"], p["same"], p["target_only"])):
            print(f"  {LABELS[kind].capitalize():<20}{len(p['create']):>8}{len(p['update']):>8}"
                  f"{p['same']:>8}{len(p['target_only']):>13}")
    for kind, p in sync.plan.items():
        for action in ("create", "update"):
            for name in p[action][:25]:
                print(f"    {action:<7}{LABELS[kind]}: {name}")
            if len(p[action]) > 25:
                print(f"    ... and {len(p[action]) - 25} more {LABELS[kind]} to {action}")
    for name, added, removed in sync.perm_changes:
        print(f"\n  Security profile '{name}' permissions:")
        for p in added:
            print(f"    + {p}")
        for p in removed:
            print(f"    - {p}")
    if sync.env_notes:
        print("\n  Lambda environment variables - review these:")
        for fn, key, note in sync.env_notes:
            print(f"    {fn}: {key} {note}")
    if sync.phone_hits and sync.changes():
        print(f"\n  Phone numbers in copied content replaced with the test number: {sync.phone_hits}")
    if sync.warnings:
        print("\n  Notes:")
        for w in sync.warnings:
            print(f"    - {w}")


def write_report(folder, sync, src_snap, tgt_snap, mode):
    lines = [f"# connect_sync report ({mode})", "",
             f"- Tool: connect_sync {VERSION}",
             f"- Run: {dt.datetime.now().isoformat(timespec='seconds')}",
             f"- Source: {src_snap['meta']['alias']} ({src_snap['meta']['account']}, {src_snap['meta']['region']})",
             f"- Target: {tgt_snap['meta']['alias']} ({tgt_snap['meta']['account']}, {tgt_snap['meta']['region']})", "",
             "## Changes made", ""] + [f"- {d}" for d in sync.done] + ["", "## Items only in the target (left alone)", ""]
    for kind, p in sync.plan.items():
        for name in p["target_only"]:
            lines.append(f"- {LABELS[kind]}: {name}")
    lines += ["", "## Security profile permission changes", ""]
    for name, added, removed in sync.perm_changes:
        lines.append(f"- {name}: added {', '.join(added) or 'none'}; removed {', '.join(removed) or 'none'}")
    lines += ["", "## Lambda environment variables to review", ""]
    lines += [f"- {fn}: {key} {note}" for fn, key, note in sync.env_notes]
    lines += ["", "## Not synced - review manually", "",
              f"- Users: {src_snap['extra'].get('users', 0)} in the source (never copied)",
              "- Phone numbers and queue caller ID numbers (never changed)",
              f"- Phone numbers inside copied content replaced with the test number: {sync.phone_hits}"]
    src_attrs, tgt_attrs = src_snap["extra"].get("attributes", {}), tgt_snap["extra"].get("attributes", {})
    for a in sorted(set(src_attrs) & set(tgt_attrs)):
        if src_attrs[a] != tgt_attrs[a]:
            lines.append(f"- Instance setting {a}: source {src_attrs[a]}, target {tgt_attrs[a]} (not changed)")
    lines += ["", "## Warnings", ""] + [f"- {w}" for w in sync.warnings]
    path = os.path.join(folder, "report.md")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


def save_backup(tgt_snap, label):
    folder = os.path.join(BACKUP_ROOT, f"{tgt_snap['meta']['alias']}-{label}-{stamp()}")
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "backup.json"), "w") as f:
        json.dump(tgt_snap, f, indent=1, default=str)
    return folder


def offer_s3_copy(account, folder):
    if not yes("Also copy the backup to S3? (CloudShell storage is removed after 120 days without use)", True):
        return
    bucket = ask("S3 bucket name (Enter to skip)")
    if not bucket:
        return
    s3 = account.client("s3")
    prefix = f"connect-sync-backups/{os.path.basename(folder)}"
    try:
        for name in os.listdir(folder):
            s3.upload_file(os.path.join(folder, name), bucket, f"{prefix}/{name}")
        print(f"  Copied to s3://{bucket}/{prefix}/")
    except API_ERRORS as e:
        print(f"  Couldn't copy to S3 ({err(e)}). The backup is still in {folder}")


def confirm(src_meta, tgt_meta, what):
    print(f"\n  Source: {src_meta['alias']}  (account {src_meta['account']}, {src_meta['region']})  - read only")
    print(f"  Target: {tgt_meta['alias']}  (account {tgt_meta['account']}, {tgt_meta['region']})  - WILL BE CHANGED")
    typed = ask(f"\nType the target alias '{tgt_meta['alias']}' to {what}, or press Enter to cancel")
    return typed == tgt_meta["alias"]


# -- wizard ---------------------------------------------------------------

def pick_instance(account, prompt, exclude=None):
    items = [i for i in account.instances() if i["Arn"] != exclude]
    if not items:
        sys.exit(f"No Amazon Connect instances found in account {account.id}, {account.region}.")
    return choose(prompt, items, lambda i: f"{i.get('InstanceAlias', '')}  ({i['Id']}, sign-in: "
                                           f"{i.get('IdentityManagementType', '?')})")


def source_account(target, region):
    print("\nWhere is the SOURCE instance (the one to copy from)?")
    where = choose("Choose", ["In this AWS account", "In another AWS account (assume a read-only role there)"], str)
    src_region = ask("Source region", region)
    if where.startswith("In this"):
        return Account(boto3.Session(region_name=src_region))
    role = ask("Role ARN in the source account (with AmazonConnectReadOnlyAccess)", required=True,
               check=lambda v: None if v.startswith("arn:aws") and ":role/" in v else "That doesn't look like a role ARN")
    external_id = ask("External ID for that role (press Enter if none)")
    kw = dict(RoleArn=role, RoleSessionName="connect-sync", DurationSeconds=3600)
    if external_id:
        kw["ExternalId"] = external_id
    creds = target.client("sts").assume_role(**kw)["Credentials"]
    return Account(boto3.Session(aws_access_key_id=creds["AccessKeyId"], aws_secret_access_key=creds["SecretAccessKey"],
                                 aws_session_token=creds["SessionToken"], region_name=src_region))


def sync_wizard(region):
    target = Account(boto3.Session(region_name=region))
    print(f"\nSigned in to account {target.id} ({region}). The TARGET instance must be in this account and region.")
    source = source_account(target, region)
    print("\nPick the SOURCE instance (copied FROM, never changed):")
    src_sum = pick_instance(source, "Source instance")
    print("\nPick the TARGET instance (brought in line with the source):")
    tgt_sum = pick_instance(target, "Target instance", exclude=src_sum["Arn"])
    src, tgt = Instance(source, src_sum, read_only=True), Instance(target, tgt_sum)
    if source.id == target.id:
        print("\n  Note: both instances are in the same AWS account. A separate account for the target is "
              "recommended, so test calls can't reach production resources.")
        if not yes("  Continue anyway?", False):
            return

    print("\nOptions")
    tgt_numbers = paged(tgt.c.list_phone_numbers_v2, "ListPhoneNumbersSummaryList", TargetArn=tgt.arn)
    print("  Every phone number found inside copied flows and quick connects (transfers, staff mobiles) is")
    print("  replaced with a test number you control, so the target can never ring a real person.")
    test_number = ask("  Test number, in +E.164 format (e.g. +6491234567)",
                      tgt_numbers[0]["PhoneNumber"] if tgt_numbers else None, required=True,
                      check=lambda v: None if E164.match(v) else "Use +E.164 format, e.g. +6491234567")
    cc = ask("  Country calling code, to also catch local numbers written without + (e.g. 64; Enter to skip)",
             check=lambda v: None if v.lstrip("+").isdigit() else "Digits only, e.g. 64")
    keep = [k.strip() for k in ask("  Flows or modules to leave untouched in the target (comma-separated names; "
                                   "Enter for none)").split(",") if k.strip()]
    bucket = ask("  S3 bucket in the target account for copying prompt audio (Enter to skip prompts)")
    print("  Lambda functions used by the flows:")
    modes = ["Leave them as they are", "Link same-named Lambdas that already exist in the target account",
             "Copy them from the source (code, settings, execution roles, DynamoDB table structures, EventBridge rules)"]
    mode = choose("  Choose", modes, str)
    lambda_mode = {modes[0]: "skip", modes[1]: "link", modes[2]: "copy"}[mode]
    boundary = ""
    if lambda_mode == "copy":
        if source.id == target.id:
            print("  Copying needs the target in a separate account (same names can't exist twice). Linking instead.")
            lambda_mode = "link"
        else:
            print("  The source role also needs read access to Lambda, IAM, DynamoDB and EventBridge for this.")
            boundary = ask("  Permissions boundary ARN for the new execution roles (Enter for none)",
                           check=lambda v: None if v.startswith("arn:aws") and ":policy/" in v else "Enter a policy ARN")
    opts = dict(test_number=test_number, country_code=cc, keep=keep, prompt_bucket=bucket, lambda_mode=lambda_mode,
                boundary=boundary, source_client=src.c, target_s3=target.client("s3"),
                target_lambda=target.client("lambda"), target_iam=target.client("iam"),
                target_dynamodb=target.client("dynamodb"), target_events=target.client("events"),
                source_lambda=source.client("lambda", True), source_iam=source.client("iam", True),
                source_dynamodb=source.client("dynamodb", True), source_events=source.client("events", True))

    print()
    src_snap, tgt_snap = snapshot(src), snapshot(tgt)
    plan = Sync(src_snap, tgt, tgt_snap, opts).run()
    print_plan(plan, "PLAN - nothing has been changed yet")
    if not plan.changes():
        print("\nThe target already matches the source. Nothing to do.")
        return
    if not confirm(src_snap["meta"], tgt_snap["meta"], "apply these changes"):
        print("Cancelled - nothing was changed.")
        return
    folder = save_backup(tgt_snap, "before-sync")
    print(f"\nBackup of the target saved to {folder}")
    offer_s3_copy(target, folder)
    print("\nApplying ...")
    result = Sync(src_snap, tgt, tgt_snap, opts, apply=True).run()
    result.plan, result.perm_changes, result.phone_hits = plan.plan, plan.perm_changes, plan.phone_hits
    result.env_notes = plan.env_notes
    report = write_report(folder, result, src_snap, tgt_snap, "sync")
    print(f"\nDone: {len(result.done)} change(s), {len(result.warnings)} warning(s).")
    print(f"Report: {report}")
    print("To undo, run this tool again and choose 'Restore'.")


def load_backup(account):
    folders = sorted(glob.glob(os.path.join(BACKUP_ROOT, "*", "backup.json")), reverse=True)
    options = folders + ["A backup in S3"]
    pick = choose("Backup to restore", options, lambda f: f if f.startswith("A backup") else os.path.dirname(f))
    if pick.startswith("A backup"):
        uri = ask("S3 URI of backup.json (s3://bucket/connect-sync-backups/.../backup.json)", required=True,
                  check=lambda v: None if v.startswith("s3://") else "Start with s3://")
        bucket, key = uri[5:].split("/", 1)
        return json.loads(account.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read())
    with open(pick) as f:
        return json.load(f)


def restore_wizard(region):
    account = Account(boto3.Session(region_name=region))
    backup = load_backup(account)
    meta = backup["meta"]
    if meta["account"] != account.id:
        sys.exit(f"That backup is from account {meta['account']}; you're signed in to {account.id}.")
    if meta["region"] != region:
        account = Account(boto3.Session(region_name=meta["region"]))
    summary = next((i for i in account.instances() if i["Id"] == meta["id"]), None)
    if not summary:
        sys.exit(f"Instance {meta['alias']} ({meta['id']}) no longer exists in {meta['region']}.")
    tgt = Instance(account, summary)
    print(f"\nBackup taken {backup.get('taken')} of {meta['alias']}.")
    current = snapshot(tgt)
    opts = dict(test_number="", country_code="", keep=[], prompt_bucket="", lambda_mode="skip")
    plan = Sync(backup, tgt, current, opts, restore=True).run()
    print_plan(plan, "RESTORE PLAN - nothing has been changed yet ('Target only' = created since the backup; "
                     "they are left in place)")
    if not plan.changes():
        print("\nThe instance already matches the backup. Nothing to do.")
        return
    if not confirm(meta, current["meta"], "restore this backup"):
        print("Cancelled - nothing was changed.")
        return
    folder = save_backup(current, "before-restore")
    print(f"\nCurrent state saved first to {folder} (so this restore can be undone too)")
    print("\nRestoring ...")
    result = Sync(backup, tgt, current, opts, apply=True, restore=True).run()
    result.plan, result.perm_changes = plan.plan, plan.perm_changes
    report = write_report(folder, result, backup, current, "restore")
    print(f"\nDone: {len(result.done)} change(s), {len(result.warnings)} warning(s). Report: {report}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help", "--version"):
        print(__doc__ if sys.argv[1] != "--version" else VERSION)
        return
    print(f"Amazon Connect sync {VERSION} - provided by Cinch + Strike as-is; try it on non-production first.")
    region = os.environ.get("AWS_REGION") or boto3.Session().region_name
    region = ask("AWS region", region, required=True)
    while True:
        print("\nWhat would you like to do?")
        action = choose("Choose", ["Sync a target instance from a source instance",
                                   "Restore a target instance from a backup", "Quit"], str)
        try:
            if action.startswith("Sync"):
                sync_wizard(region)
            elif action.startswith("Restore"):
                restore_wizard(region)
            else:
                return
        except API_ERRORS as e:
            print(f"\nStopped: {err(e)}")


if __name__ == "__main__":
    main()
