#!/usr/bin/env python3
"""Resident paper-only converter from ranked wallets to exact-policy rotation evidence."""
from __future__ import annotations
import argparse, json, subprocess, sys, time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from src.wallet_copy.store import atomic_write_json
from src.wallet_copy.models import WalletEvent
from src.wallet_copy.profit_engine import CandidatePolicy, intents_for_policy

POLICY="fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
FEE_RATE=0.069997697
def _load(path:Path,default:Any)->Any:
    try:return json.loads(path.read_text())
    except Exception:return default
def _ts(value:Any)->float:
    try:
        if isinstance(value,(int,float)):return float(value)
        return datetime.fromisoformat(str(value).replace("Z","+00:00")).timestamp()
    except (TypeError,ValueError):return 0.0
def _jsonl(path:Path)->list[dict[str,Any]]:
    rows=[]
    if not path.exists():return rows
    for line in path.open(encoding="utf-8",errors="replace"):
        try:row=json.loads(line)
        except json.JSONDecodeError:continue
        if isinstance(row,dict):rows.append(row)
    return rows
def _replay(events:list[dict[str,Any]],resolutions:list[dict[str,Any]])->dict[str,dict[str,Any]]:
    token_meta={}
    for row in resolutions:
        direction=str(row.get("direction") or row.get("winning_outcome") or "").lower()
        yes,no=str(row.get("yes_token") or ""),str(row.get("no_token") or "")
        common={"condition_id":str(row.get("condition_id") or ""),"market_slug":str(row.get("market_slug") or "")}
        if yes:token_meta[yes]=common|{"outcome":"Up","won":direction=="up"}
        if no:token_meta[no]=common|{"outcome":"Down","won":direction=="down"}
    policy=CandidatePolicy(policy_id=POLICY,min_price=.01,max_price=1.0,min_wallet_usdc=0,max_wallet_usdc=0,
      min_seconds_from_open=None,max_seconds_from_open=None,wallet_fraction=.10,max_order_usd=4.0,min_order_usd=1.0)
    by_wallet={}
    seen=set()
    for row in events:
        if row.get("event")!="top10_broad_paper_measurement" or str(row.get("side") or "").upper()!="BUY":continue
        identity=f"{row.get('transaction_hash')}|{row.get('asset')}"
        if identity in seen:continue
        seen.add(identity);wallet=str(row.get("wallet") or "").lower()
        if not wallet:continue
        acc=by_wallet.setdefault(wallet,{"event_ids":[],"raw_own_source_buys":0,"eligible_intents":0,"would_submit_executable":0,
          "pending_resolution":0,"resolved_post_fee_windows":0,"post_fee_pnl_usd":0.0,"failed_policy_events":0})
        acc["event_ids"].append(identity);acc["raw_own_source_buys"]+=1
        source_size=float(row.get("source_size") or 0);asset=str(row.get("asset") or "")
        meta=token_meta.get(asset)
        if not meta:acc["pending_resolution"]+=1;continue
        event=WalletEvent(source_wallet=wallet,wallet_name="",row_type="TRADE",action="BUY",
          condition_id=meta["condition_id"],market_slug=meta["market_slug"],outcome=meta["outcome"],
          price=float(row.get("source_price") or 0),size=source_size,
          usdc_size=source_size*float(row.get("source_price") or 0),event_ts=float(row.get("block_ts") or 0),
          observed_ts=float(row.get("received_at_s") or row.get("block_ts") or 0),event_id=identity,
          token_id=asset,transaction_hash=str(row.get("transaction_hash") or ""),raw=row)
        intents=intents_for_policy([event],policy)
        if not intents:acc["failed_policy_events"]+=1;continue
        intent=intents[0];acc["eligible_intents"]+=1
        result=row.get("result") if isinstance(row.get("result"),dict) else {}
        if result.get("status")!="FILLED":continue
        acc["would_submit_executable"]+=1
        book=result.get("book") if isinstance(result.get("book"),dict) else {}
        price=float(book.get("avg_fill_price") or row.get("source_price") or 0)
        shares=float(book.get("fillable_shares") or intent.shares)
        cost=float(book.get("fillable_usd") or intent.copy_size_usd)
        fee=FEE_RATE*shares*price*(1-price)
        pnl=(shares if meta["won"] else 0)-cost-fee
        acc["resolved_post_fee_windows"]+=1;acc["post_fee_pnl_usd"]+=pnl
    for acc in by_wallet.values():acc["post_fee_pnl_usd"]=round(acc["post_fee_pnl_usd"],6)
    return by_wallet
def build_report(queue:dict[str,Any], lanes:dict[str,Any], *, generated_at:str, direct:dict[str,Any]|None=None,
                 direct_events:list[dict[str,Any]]|None=None,resolutions:list[dict[str,Any]]|None=None)->dict[str,Any]:
    direct=direct or {}
    direct_by_wallet={str(k).lower():v for k,v in (direct.get("wallets") or {}).items() if isinstance(v,dict)}
    replay=_replay(direct_events or [],resolutions or [])
    lane_by_wallet={str(r.get("wallet") or "").lower():r for r in lanes.get("lanes",[]) if isinstance(r,dict)}
    queue_rows=queue.get("ranked_members") or queue.get("rows") or queue.get("wallets") or queue.get("queue") or []
    candidates=[]
    for rank,row in enumerate(queue_rows[:50],1):
        if not isinstance(row,dict):continue
        wallet=str(row.get("wallet") or row.get("address") or "").lower()
        if not wallet:continue
        lane=lane_by_wallet.get(wallet,{})
        source=direct_by_wallet.get(wallet,{})
        measured=replay.get(wallet,{})
        gates=dict(lane.get("live_canary_packet_preconditions") or {})
        raw_buys=int(measured.get("raw_own_source_buys") or 0)
        eligible=int(measured.get("eligible_intents") or 0)
        resolved=int(measured.get("resolved_post_fee_windows") or 0)
        exact_sample=resolved>0 and eligible>0
        if not gates:
            last_event=float(source.get("last_event_ts") or 0)
            gates={
              "fresh_source_active_window":last_event>0 and (_ts(generated_at)-last_event)<86400,
              "materially_nonzero_acceptance":int(measured.get("would_submit_executable") or 0)>0,
              "exact_policy_resolution_sample_gte_50":resolved>=50,
              "post_fee_economics_positive":float(measured.get("post_fee_pnl_usd") or 0)>0,
            }
        failed=[key for key,value in gates.items() if not value]
        if not exact_sample: failed.append("exact_policy_resolution_sample")
        candidates.append({"rank":rank,"wallet":wallet,"policy_id":POLICY,
          "raw_own_source_buys":raw_buys,"eligible_intents":eligible,
          "would_submit_executable":int(measured.get("would_submit_executable") or 0),
          "resolved_post_fee_windows":resolved,
          "pending_resolution":int(measured.get("pending_resolution") or 0),
          "post_fee_pnl_usd":float(measured.get("post_fee_pnl_usd") or 0),
          "event_identity_count":len(measured.get("event_ids") or []),
          "weekday_slice":row.get("weekday_slice") or row.get("temporal_classification"),
          "source_freshness_h":lane.get("external_latest_trade_age_h"),
          "direct_clob_source":{"updated_at":direct.get("updated_at"),"last_event_ts":source.get("last_event_ts"),
            "route":(direct.get("source") or {}).get("direct_clob_base_url"),"sample_status":source.get("sample_status")},
          "fee_resolution_attachment":{"fee_rate":0.069997697,"fee_formula":"rate*shares*price*(1-price)",
            "canonical_resolution_required":True,"attached":exact_sample,"source":"btc_resolutions_from_btcusdt_ticks"},
          "failed_admission_gates":list(dict.fromkeys(failed)),
          "all_mechanical_gates_pass":bool(gates) and not failed and exact_sample})
    ready=[r for r in candidates if r["all_mechanical_gates_pass"]]
    return {"kind":"ranked_successor_exact_policy_resolution_shadow","generated_at":generated_at,
      "flow_stage":"PROMOTE/ROTATE/OBSERVE","paper_only":True,"live_orders_allowed":False,
      "live_mutation":False,"single_submitter_unchanged":True,"copy_intent_parity":True,
      "policy_id":POLICY,"candidates":candidates,"qualified_successors":ready,
      "exact_policy_replay":{"policy_id":POLICY,"builder":"src.wallet_copy.profit_engine.intents_for_policy",
        "source":"event-derived direct-CLOB own-source BUY service",
        "direct_clob_updated_at":direct.get("updated_at"),"canonical_fee_resolution_required":True},
      "automatic_handoff":{"target":"existing clearance/rotation packet","fired":False,
        "actuator":"report_ranked_queue_clearance_packets.py -> report_active_set_rotation_packet.py"},
      "status":"QUALIFIED_SUCCESSOR_READY" if ready else "ACCRUING"}
def once(args:argparse.Namespace)->None:
    subprocess.run([sys.executable,"scripts/run_ready_wallet_shadow_lanes.py"],cwd=ROOT,check=True,stdout=subprocess.DEVNULL)
    now=datetime.now(tz=UTC).isoformat().replace("+00:00","Z")
    report=build_report(_load(Path(args.queue),{}),_load(Path(args.lanes),{}),generated_at=now,
      direct=_load(Path(args.direct_clob),{}),direct_events=_jsonl(Path(args.direct_events)),
      resolutions=_jsonl(Path(args.resolutions)))
    if report["qualified_successors"]:
        replay_pass=list(report["qualified_successors"])
        subprocess.run([sys.executable,"scripts/build_queue_clearance_gaps.py","--limit","50"],cwd=ROOT,check=True)
        subprocess.run([sys.executable,"scripts/report_ranked_queue_clearance_packets.py","--count","50"],cwd=ROOT,check=True)
        subprocess.run([sys.executable,"scripts/build_full_pool_member_queue.py","--limit","0"],cwd=ROOT,check=True)
        subprocess.run([sys.executable,"scripts/report_ranked_queue_clearance_packets.py","--count","50"],cwd=ROOT,check=True)
        subprocess.run([sys.executable,"scripts/report_active_set_rotation_packet.py"],cwd=ROOT,check=True)
        clearance=_load(ROOT/"data/research/ranked_queue_clearance_packets_latest.json",{})
        ready_wallets={str(row.get("wallet") or "").lower() for row in clearance.get("packets",[])
          if isinstance(row,dict) and bool((row.get("clearance") or {}).get("ready_for_live"))
          and bool((row.get("clearance") or {}).get("hot_standby_ready"))}
        report["replay_gate_candidates"]=replay_pass
        report["qualified_successors"]=[row for row in replay_pass if row["wallet"] in ready_wallets]
        report["automatic_handoff"]["artifacts_rebuilt"]=True
        report["automatic_handoff"]["fired"]=bool(report["qualified_successors"])
        report["status"]="QUALIFIED_SUCCESSOR_READY" if report["qualified_successors"] else "ACCRUING_CLEARANCE"
    atomic_write_json(Path(args.output),report);print(json.dumps({"generated_at":now,"status":report["status"],"candidates":len(report["candidates"])}),flush=True)
def main()->int:
    p=argparse.ArgumentParser();p.add_argument("--queue",default="data/research/wallet_copy_full_pool_member_queue.json")
    p.add_argument("--lanes",default="data/research/wallet_copy_ready_shadow_lanes_state.json")
    p.add_argument("--direct-clob",default="data/research/wallet_copy_top10_broad_paper_measurement_state.json")
    p.add_argument("--direct-events",default="data/research/wallet_copy_top10_broad_paper_events.jsonl")
    p.add_argument("--resolutions",default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    p.add_argument("--output",default="data/research/ranked_successor_exact_policy_resolution_shadow_latest.json")
    p.add_argument("--interval-s",type=float,default=0);args=p.parse_args()
    while True:
        once(args)
        if args.interval_s<=0:return 0
        time.sleep(args.interval_s)
if __name__=="__main__":raise SystemExit(main())
