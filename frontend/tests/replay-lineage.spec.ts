import { expect, test } from '@playwright/test'

/**
 * 死信重放 × 流式血缘联动验收：
 * 1. 死信处理页执行标记重放，列表显示已重放与重放批次；
 * 2. 切到流式血缘页，异常标记被清除，图上只保留本批次的有效边（无重复边）；
 * 3. 中断（start）后恢复（resume）同一批次，仍只有一个可查询边集；
 * 4. 两侧的重放血缘回执可查询且批次一致。
 */

const BASE = process.env.E2E_BASE_URL || 'http://127.0.0.1:5173'

async function apiJson(path: string, init?: RequestInit) {
  const response = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  return response.json()
}

test('死信重放后流式血缘页同步有效边并清除异常标记', async ({ page }) => {
  test.setTimeout(120_000)
  const browserErrors: string[] = []
  page.on('pageerror', (e) => browserErrors.push(e.message))
  page.on('response', (r) => {
    if (r.url().includes('/api/') && r.status() >= 400) {
      browserErrors.push(`${r.status()} ${r.url()}`)
    }
  })

  // 重放前：流式血缘页存在异常节点
  await page.goto('/stream_lineage')
  await expect(page.locator('svg.dag-svg')).toBeVisible()
  const beforeGraph = await apiJson('/api/stream_lineage')
  const abnormalTopicNodes = beforeGraph.nodes.filter((n: any) => n.abnormal)
  expect(abnormalTopicNodes.length).toBeGreaterThan(0)
  const targetDeadLetterId = 1 // 种子 DEAD-0001 / TOPI-0001

  // 死信处理页执行标记重放
  await page.goto('/dead_letter')
  const targetRow = page.getByTestId(`row-${targetDeadLetterId}`)
  await targetRow.getByRole('button', { name: '标记重放', exact: true }).click()
  await expect(targetRow).toHaveAttribute('data-status', '已重放')
  await expect(page.getByTestId(`replay-batch-${targetDeadLetterId}`)).toBeVisible()
  const batchText = await page.getByTestId(`replay-batch-${targetDeadLetterId}`).innerText()
  const batchNo = batchText.replace('重放批次 ', '')

  // 死信侧回执可查，保留重放批次与有效血缘边
  const deadReceipts = await apiJson(`/api/dead_letter/${targetDeadLetterId}/replay_receipts`)
  expect(deadReceipts.total).toBe(1)
  expect(deadReceipts.items[0].replay_batch).toBe(batchNo)
  expect(deadReceipts.items[0].valid_edges.length).toBeGreaterThan(0)

  // 切换到流式血缘页核对：异常标记清除、有效边带批次、无重复边
  await page.goto('/stream_lineage')
  await expect(page.locator('svg.dag-svg')).toBeVisible()
  const afterGraph = await apiJson('/api/stream_lineage')
  const topicNodes = afterGraph.nodes.filter((n: any) => n['上游主题'] === 'TOPI-0001')
  expect(topicNodes.length).toBeGreaterThan(0)
  expect(topicNodes.every((n: any) => !n.abnormal)).toBe(true)
  const topicEdges = afterGraph.edges.filter((e: any) => e.replay_batch === batchNo)
  expect(topicEdges.length).toBe(1)
  const activeEdges = (await apiJson('/api/stream_lineage/edges?active=true')).items
  const activeTopicEdges = activeEdges.filter((e: any) => e.source_topic === 'TOPI-0001')
  expect(activeTopicEdges.length).toBe(1)

  // 流式血缘侧回执来源记录与死信侧一致
  const lineageRefs = await apiJson('/api/stream_lineage/replay_receipts')
  const ref = lineageRefs.items.find((r: any) => r.replay_batch === batchNo)
  expect(ref).toBeTruthy()
  expect(ref.edge_codes).toEqual(topicEdges.map((e: any) => e.edge_code))

  // 再次重放不产生重复边
  await page.goto('/dead_letter')
  await page.getByTestId(`row-${targetDeadLetterId}`).getByRole('button', { name: '标记重放', exact: true }).click()
  const edgesAfterRetry = (await apiJson('/api/stream_lineage/edges?active=true')).items
  expect(edgesAfterRetry.filter((e: any) => e.source_topic === 'TOPI-0001').length).toBe(1)

  expect(browserErrors).toEqual([])
})

test('中断后恢复同一重放批次：无重复边且异常清除', async () => {
  const targetDeadLetterId = 2 // 种子 DEAD-0002 / TOPI-0002

  const started = await apiJson(`/api/dead_letter/${targetDeadLetterId}/replay/start`, { method: 'POST' })
  expect(started.ok).toBe(true)
  const batchNo = started.entry.replay_batch
  expect(started.entry.receipt_status).toBe('进行中')

  // 中断期间处置状态不变，且无有效边
  const pending = await apiJson(`/api/dead_letter/${targetDeadLetterId}`)
  expect(pending.status).toBe('待处置')
  const midEdges = (await apiJson('/api/stream_lineage/edges')).items
  expect(midEdges.filter((e: any) => e.source_topic === 'TOPI-0002' && e.edge_status === '有效' && e.active).length).toBe(0)

  // 恢复
  const resumed = await apiJson(`/api/dead_letter/${targetDeadLetterId}/replay/resume`, { method: 'POST' })
  expect(resumed.ok).toBe(true)
  expect(resumed.entry.replay_batch).toBe(batchNo)
  expect(resumed.entry.receipt_status).toBe('已完成')

  const done = await apiJson(`/api/dead_letter/${targetDeadLetterId}`)
  expect(done.status).toBe('已重放')

  const graph = await apiJson('/api/stream_lineage')
  const topicNodes = graph.nodes.filter((n: any) => n['上游主题'] === 'TOPI-0002')
  expect(topicNodes.every((n: any) => !n.abnormal)).toBe(true)
  const edges = (await apiJson('/api/stream_lineage/edges?active=true')).items
  const topicEdges = edges.filter((e: any) => e.source_topic === 'TOPI-0002')
  expect(topicEdges.length).toBe(1)
  expect(topicEdges[0].replay_batch).toBe(batchNo)

  // 只有一份回执
  const receipts = await apiJson(`/api/dead_letter/${targetDeadLetterId}/replay_receipts`)
  expect(receipts.total).toBe(1)
})
