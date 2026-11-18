/**
 * 序号6补数去重工作台接口封装。
 *
 * 对应后端 /api/backfill/* 专用路由：统一提交、合并清理、暂停/恢复/重试、
 * 详情面板、清单复核、迟到旁路版本与修复台账。
 */
import { request } from './client'

export type RangeTuple = [number, number]

export type MergeLog = {
  at: number
  event: string
  before_ranges?: RangeTuple[]
  after_ranges?: RangeTuple[]
  absorbed_jobs?: { job_id: number; job_name: string }[] | number[]
  incoming_ranges?: RangeTuple[]
  trigger_source?: string
  child_job_id?: number
}

export type SideOutputRef = {
  side_output_key: string
  version: number | null
  ranges: RangeTuple[]
  status: string
}

export type BackfillDetail = {
  id: number
  作业名称: string
  目标指标: string
  status: string
  进度比例: string
  补数范围: string
  起始位点: string
  截止位点: string
  residual: boolean
  ineffective: boolean
  trigger_source: string
  active_ranges: RangeTuple[]
  completed_ranges: RangeTuple[]
  side_output_ref: SideOutputRef | null
  source_versions: SideOutputRef[]
  request_keys: string[]
  merge_logs: MergeLog[]
  parent_job: number | null
  child_jobs: { job_id: number; job_name: string; status: string; active_ranges: RangeTuple[] }[]
  paused_progress_pct: number | null
  merged_into: number | null
  retry_token: string | null
  verified_with_aggregate?: number
}

export type ReviewItem = {
  target_metric: string
  active_job_count: number
  unique_active_scope: boolean
  active_ranges: RangeTuple[]
  completed_ranges: RangeTuple[]
  side_output_current_version: number | null
  referenced_versions: number[]
  single_side_output_ref: boolean
  completed_reentry: boolean
  paused_jobs: { job_id: number; paused_progress_pct: number | null; current_progress_pct: number }[]
  progress_non_regressive: boolean
  child_jobs: number[]
}

export type ReviewChecklist = {
  ok: boolean
  items: ReviewItem[]
  residual_jobs: { job_id: number; job_name: string }[]
  ledger_entries: number
  all_converged: boolean
}

export type SideOutputVersion = {
  side_output_key: string
  target_metric: string
  version: number
  ranges: RangeTuple[]
  source: string
  entry_count: number
  created_at: number
  status: string
}

export type LedgerEntry = {
  ledger_id: number
  created_at: number
  module: string
  related_modules: string[]
  repair_seq: string
  action: string
  detail: Record<string, unknown>
}

async function asJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let message = '补数工作台请求失败'
    try {
      const error = await response.json()
      message = typeof error.detail === 'object'
        ? JSON.stringify(error.detail)
        : String(error.detail ?? message)
    } catch {
      // 非 JSON 错误体时保留默认消息
    }
    throw new Error(message)
  }
  return response.json() as Promise<T>
}

export function formatRanges(ranges: RangeTuple[] | undefined): string {
  if (!ranges || ranges.length === 0) return '—'
  return ranges.map(([lo, hi]) => `${lo}-${hi}`).join('，')
}

export async function submitBackfill(payload: {
  target_metric: string
  start_offset: number
  end_offset: number
  trigger_source?: string
  request_key?: string
  job_name?: string
  mode?: 'absorb' | 'parent_child'
}): Promise<Record<string, unknown>> {
  const response = await request('/api/backfill/submit', {
    method: 'POST',
    body: JSON.stringify({ trigger_source: 'manual', mode: 'absorb', ...payload }),
  })
  return asJson(response)
}

export async function mergeBackfillJobs(targetMetric?: string): Promise<Record<string, unknown>> {
  const query = targetMetric ? `?target_metric=${encodeURIComponent(targetMetric)}` : ''
  const response = await request(`/api/backfill/merge${query}`, {
    method: 'POST',
    body: JSON.stringify({ target_metric: targetMetric ?? null }),
  })
  return asJson(response)
}

export async function pauseBackfill(jobId: number): Promise<Record<string, unknown>> {
  return asJson(await request(`/api/backfill/${jobId}/pause`, { method: 'POST' }))
}

export async function resumeBackfill(jobId: number): Promise<Record<string, unknown>> {
  return asJson(await request(`/api/backfill/${jobId}/resume`, { method: 'POST' }))
}

export async function retryBackfill(
  jobId: number,
  retryToken?: string,
): Promise<Record<string, unknown>> {
  return asJson(
    await request(`/api/backfill/${jobId}/retry`, {
      method: 'POST',
      body: JSON.stringify({ retry_token: retryToken ?? null }),
    }),
  )
}

export async function fetchBackfillDetail(jobId: number): Promise<{ ok: boolean; job: BackfillDetail }> {
  return asJson(await request(`/api/backfill/${jobId}/detail`))
}

export async function fetchReviewChecklist(targetMetric?: string): Promise<ReviewChecklist> {
  const query = targetMetric ? `?target_metric=${encodeURIComponent(targetMetric)}` : ''
  return asJson(await request(`/api/backfill/review/checklist${query}`))
}

export async function cleanupResidual(): Promise<Record<string, unknown>> {
  return asJson(await request('/api/backfill/residual/cleanup', { method: 'POST' }))
}

export async function fetchSideOutputVersions(
  targetMetric?: string,
): Promise<{ items?: SideOutputVersion[]; current?: SideOutputVersion | null; versions?: SideOutputVersion[] }> {
  const query = targetMetric ? `?target_metric=${encodeURIComponent(targetMetric)}` : ''
  return asJson(await request(`/api/backfill/side-output/versions${query}`))
}

export async function fetchRepairLedger(module?: string): Promise<{ total: number; items: LedgerEntry[] }> {
  const query = module ? `?module=${encodeURIComponent(module)}` : ''
  return asJson(await request(`/api/backfill/repair/ledger${query}`))
}
