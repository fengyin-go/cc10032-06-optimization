<template>
  <section class="backfill-workbench" data-testid="backfill-workbench">
    <div class="workbench-toolbar">
      <strong>序号6 · 迟到补数去重工作台</strong>
      <div class="toolbar-actions">
        <button class="btn" type="button" data-testid="bf-merge" @click="onMerge">
          合并清理任务清单
        </button>
        <button class="btn" type="button" data-testid="bf-review" @click="loadReview">
          复核唯一活动范围
        </button>
        <button class="btn ghost" type="button" data-testid="bf-residual" @click="onCleanupResidual">
          隔离6号残留数据
        </button>
      </div>
    </div>

    <p v-if="message" class="workbench-message" :class="messageKind" data-testid="bf-message">
      {{ message }}
    </p>

    <div class="review-panel" data-testid="bf-review-panel">
      <h4>补数任务清单复核</h4>
      <table v-if="review" class="data-table compact">
        <thead>
          <tr>
            <th>目标指标</th>
            <th>活动作业数</th>
            <th>唯一活动范围</th>
            <th>已完成区间</th>
            <th>旁路版本</th>
            <th>完成区间重入</th>
            <th>进度不倒退</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in review.items" :key="item.target_metric" :data-testid="`review-${item.target_metric}`">
            <td>{{ item.target_metric }}</td>
            <td>{{ item.active_job_count }}</td>
            <td>
              <span :class="badge(item.unique_active_scope)">{{ item.unique_active_scope ? '唯一' : '重复' }}</span>
              {{ formatRanges(item.active_ranges) }}
            </td>
            <td>{{ formatRanges(item.completed_ranges) }}</td>
            <td>
              <span :class="badge(item.single_side_output_ref)">
                当前 v{{ item.side_output_current_version ?? '—' }}
              </span>
              <span v-if="item.referenced_versions.length > 1" class="muted">
                （引用：{{ item.referenced_versions.map((v) => `v${v}`).join(', ') }}）
              </span>
            </td>
            <td>
              <span :class="badge(!item.completed_reentry)">{{ item.completed_reentry ? '异常重入' : '未重入' }}</span>
            </td>
            <td>
              <span :class="badge(item.progress_non_regressive)">
                {{ item.progress_non_regressive ? '未倒退' : '倒退' }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-else class="muted">点击“复核唯一活动范围”加载清单。</p>
      <p v-if="review" class="muted">
        残留作业 {{ review.residual_jobs.length }} 条 · 修复台账 {{ review.ledger_entries }} 条 ·
        整体收敛：<span :class="badge(review.all_converged)">{{ review.all_converged ? '是' : '否' }}</span>
      </p>
    </div>

    <div v-if="detail" class="modal-backdrop" data-testid="bf-detail-modal" @click.self="detail = null">
      <div class="modal detail-modal">
        <h3>补数作业详情 #{{ detail.id }}</h3>
        <p v-if="detail.ineffective" class="workbench-message error" data-testid="bf-ineffective">
          该作业为6号残留数据，位点不可解析，已隔离且不参与补数计算。
        </p>
        <dl class="detail-grid">
          <dt>作业名称</dt><dd>{{ detail.作业名称 }}</dd>
          <dt>目标指标</dt><dd>{{ detail.目标指标 }}</dd>
          <dt>状态</dt><dd>{{ detail.status }}（{{ detail.trigger_source === 'auto' ? '自动触发' : detail.trigger_source === 'manual' ? '人工补数' : detail.trigger_source }}）</dd>
          <dt>进度比例</dt><dd>{{ detail.进度比例 }}<span v-if="detail.paused_progress_pct != null" class="muted">（暂停快照 {{ detail.paused_progress_pct }}%）</span></dd>
          <dt>活动范围</dt><dd data-testid="bf-detail-active">{{ formatRanges(detail.active_ranges) }}</dd>
          <dt>完成区间</dt><dd>{{ formatRanges(detail.completed_ranges) }}</dd>
          <dt>当前旁路版本</dt>
          <dd>
            v{{ detail.side_output_ref?.version ?? '—' }}
            <span class="muted">（{{ detail.side_output_ref?.side_output_key }}）</span>
          </dd>
          <dt>清单来源版本</dt>
          <dd>
            <span v-for="src in detail.source_versions" :key="src.version ?? src.side_output_key" class="version-chip">
              v{{ src.version }}
            </span>
            <span v-if="detail.source_versions.length === 0" class="muted">无</span>
          </dd>
          <dt v-if="detail.parent_job != null">父作业</dt><dd v-if="detail.parent_job != null">#{{ detail.parent_job }}</dd>
          <dt v-if="detail.child_jobs.length">子作业</dt>
          <dd v-if="detail.child_jobs.length">
            <span v-for="child in detail.child_jobs" :key="child.job_id" class="version-chip">
              #{{ child.job_id }} {{ child.status }}
            </span>
          </dd>
          <dt v-if="detail.merged_into != null">并入作业</dt><dd v-if="detail.merged_into != null">#{{ detail.merged_into }}</dd>
        </dl>

        <div class="merge-log" data-testid="bf-merge-log">
          <h4>合并前后范围记录</h4>
          <ul v-if="detail.merge_logs.length">
            <li v-for="(log, idx) in detail.merge_logs" :key="idx">
              <strong>{{ log.event }}</strong>
              <template v-if="log.before_ranges"> 前：{{ formatRanges(log.before_ranges) }}</template>
              <template v-if="log.after_ranges"> → 后：{{ formatRanges(log.after_ranges) }}</template>
              <template v-if="log.child_job_id"> → 子作业 #{{ log.child_job_id }}</template>
            </li>
          </ul>
          <p v-else class="muted">暂无合并记录。</p>
        </div>

        <div class="page-actions">
          <button class="btn" type="button" data-testid="bf-detail-pause" @click="onPause(detail.id)">暂停</button>
          <button class="btn" type="button" data-testid="bf-detail-resume" @click="onResume(detail.id)">恢复</button>
          <button class="btn" type="button" data-testid="bf-detail-retry" @click="onRetry(detail.id)">重试</button>
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  cleanupResidual,
  fetchBackfillDetail,
  fetchReviewChecklist,
  formatRanges,
  mergeBackfillJobs,
  pauseBackfill,
  resumeBackfill,
  retryBackfill,
  type BackfillDetail,
  type ReviewChecklist,
} from '@/api/backfill'

const emit = defineEmits<{ (event: 'changed'): void }>()

const review = ref<ReviewChecklist | null>(null)
const detail = ref<BackfillDetail | null>(null)
const message = ref('')
const messageKind = ref('info')

function notify(text: string, kind: 'info' | 'error' | 'success' = 'info') {
  message.value = text
  messageKind.value = kind
}

function badge(ok: boolean): string {
  return ok ? 'badge ok' : 'badge warn'
}

async function loadReview() {
  try {
    review.value = await fetchReviewChecklist()
    notify('复核完成：活动范围、旁路引用与完成比例已刷新', 'success')
  } catch (error) {
    notify(error instanceof Error ? error.message : '复核失败', 'error')
  }
}

async function onMerge() {
  try {
    const result = await mergeBackfillJobs()
    notify(
      `合并清理完成：涉及 ${String(result.metrics_with_duplicates)} 个指标，归并 ${String(result.merged_jobs)} 条作业`,
      'success',
    )
    await loadReview()
    emit('changed')
  } catch (error) {
    notify(error instanceof Error ? error.message : '合并失败', 'error')
  }
}

async function onCleanupResidual() {
  try {
    const result = await cleanupResidual()
    const jobs = (result.residual_jobs as unknown[]) ?? []
    notify(`已隔离 ${jobs.length} 条6号残留数据`, 'success')
    await loadReview()
    emit('changed')
  } catch (error) {
    notify(error instanceof Error ? error.message : '残留清理失败', 'error')
  }
}

async function openDetail(jobId: number) {
  try {
    const payload = await fetchBackfillDetail(jobId)
    detail.value = payload.job
  } catch (error) {
    notify(error instanceof Error ? error.message : '详情读取失败', 'error')
  }
}

async function onPause(jobId: number) {
  try {
    await pauseBackfill(jobId)
    notify(`作业 #${jobId} 已暂停，进度快照保留`, 'success')
    await openDetail(jobId)
    await loadReview()
    emit('changed')
  } catch (error) {
    notify(error instanceof Error ? error.message : '暂停失败', 'error')
  }
}

async function onResume(jobId: number) {
  try {
    await resumeBackfill(jobId)
    notify(`作业 #${jobId} 已恢复，完成比例未倒退`, 'success')
    await openDetail(jobId)
    await loadReview()
    emit('changed')
  } catch (error) {
    notify(error instanceof Error ? error.message : '恢复失败', 'error')
  }
}

async function onRetry(jobId: number) {
  try {
    const result = await retryBackfill(jobId)
    notify(
      result.idempotent
        ? `作业 #${jobId} 相同重试已生效，本次并发请求幂等跳过`
        : `作业 #${jobId} 已重新入队，已完成区间不重入`,
      'success',
    )
    await openDetail(jobId)
    emit('changed')
  } catch (error) {
    notify(error instanceof Error ? error.message : '重试失败', 'error')
  }
}

defineExpose({ openDetail, loadReview })
</script>

<style scoped>
.backfill-workbench {
  margin: 16px 0;
  border: 1px solid var(--border-color, #d9dde5);
  border-radius: 10px;
  padding: 14px 16px;
  background: var(--panel-bg, #fafbfd);
}

.workbench-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.toolbar-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.workbench-message {
  margin: 10px 0 0;
  padding: 8px 10px;
  border-radius: 6px;
}

.workbench-message.success {
  color: #176b3a;
  background: #e7f6ec;
}

.workbench-message.error {
  color: #a32d2d;
  background: #fdeaea;
}

.review-panel {
  margin-top: 14px;
}

.data-table.compact {
  font-size: 13px;
}

.badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 12px;
  margin-right: 6px;
}

.badge.ok {
  color: #176b3a;
  background: #e7f6ec;
}

.badge.warn {
  color: #9a5b13;
  background: #fdf1dd;
}

.muted {
  color: #7a818e;
}

.detail-modal {
  max-width: 720px;
  width: min(720px, 92vw);
  max-height: 86vh;
  overflow: auto;
}

.detail-grid {
  display: grid;
  grid-template-columns: 110px 1fr;
  gap: 6px 12px;
  margin: 12px 0;
}

.detail-grid dt {
  color: #7a818e;
}

.detail-grid dd {
  margin: 0;
}

.version-chip {
  display: inline-block;
  padding: 1px 8px;
  margin: 0 6px 4px 0;
  border-radius: 999px;
  background: #eef2ff;
  color: #3046a3;
  font-size: 12px;
}

.merge-log {
  border-top: 1px dashed #d9dde5;
  padding-top: 10px;
}

.merge-log ul {
  padding-left: 18px;
  margin: 6px 0;
}
</style>
