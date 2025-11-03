import React, { useEffect, useRef, useState } from 'react'
import Confetti from 'react-confetti'
import { motion, AnimatePresence } from 'framer-motion'

const PROFILES = [
  {
    key: 'SAFE',
    title: 'Low Exposure Portfolio (Limited Regulatory Impact)',
    intro:
      'Companies with low exposure to the new regulation based on combined NLP–Quant analysis. Defensive tilt with lower post-announcement volatility. For investors prioritizing stability and protection against regulatory uncertainty.',
    bullets: [
      'Sector tilt: Defensive mix with emphasis on Financial Services and Health Care defensives across the broader universe.',
      'Risk profile: Low return volatility following the regulatory announcement; reduced sensitivity to policy shocks.',
      '30-stock lens: The least volatile names over the 30-day window are predominantly in Health Care, reinforcing its defensive role.',
    ],
  },
  {
    key: 'MEDIUM',
    title: 'Moderate Exposure Portfolio (Average Regulatory Impact)',
    intro:
      'Companies moderately affected by the legislation, including spillover from adjacent industries. Balanced sensitivity with a controlled risk–return profile suitable for measured upside.',
    bullets: [
      'Sector tilt: Primarily Technology and Industrials, reflecting intermediate sensitivity and diversified drivers.',
      'Risk profile: Partial responsiveness to regulation while maintaining balance between drawdown control and participation.',
      '30-stock lens: Most stable 30-day performances concentrated in Technology and Industrials; in the full universe these sectors show moderate sensitivity.',
    ],
  },
  {
    key: 'RISKY',
    title: 'High Exposure / Opportunistic Portfolio (Strong Regulatory Impact)',
    intro:
      'Targets companies highly exposed to the new regulation due to core activities, footprint, or industry structure. Built for investors willing to assume greater risk to capture market dislocations or adaptive upside.',
    bullets: [
      'Sector tilt: Consumer, Technology, and Financial Services dominate, reflecting direct regulatory channels and higher beta.',
      'Risk profile: Elevated volatility and stronger market beta post-announcement; larger dispersion of outcomes.',
      '30-stock lens: Consumer names are most represented among the 30 most impacted, a pattern that persists in the broader universe.',
    ],
  },
]

const STATIC_PREFIX = (import.meta && import.meta.env && import.meta.env.VITE_STATIC_PREFIX) || '/static'
const BASE_URL = (import.meta && import.meta.env && import.meta.env.BASE_URL) || '/'
const join = (...parts) =>
  parts
    .join('/')
    .replace(/\/{2,}/g, '/')
    .replace(/\/(\?|#)/, '$1')
export const assetUrl = (relativePath) => {
  const base = BASE_URL.endsWith('/') ? BASE_URL : BASE_URL + '/'
  const prefix = STATIC_PREFIX.startsWith('/') ? STATIC_PREFIX.slice(1) : STATIC_PREFIX
  const rel = relativePath.replace(/^\/+/, '')
  return join(base, prefix, rel)
}

const isAllowed = (f) => !!f && /\.(html?|xml)$/i.test(f.name)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const RISK_TO_PREFIX = {
  SAFE: 'faible',
  MEDIUM: 'moyen',
  RISKY: 'fort',
}

const ASSETS = {
  BEFORE: (risk) => {
    const p = RISK_TO_PREFIX[risk]
    return [
      {
        title: 'Sector Allocation Overview',
        caption:
          'Distribution of exposure by sector prior to any legislative-risk adjustment. The Full view spans the complete universe; Top 30 isolates the most exposed names.',
        files: [`sector_dist/${p}_dist_full.html`, `sector_dist/${p}_dist_top30.html`],
      },
      {
        title: 'Top 30 Constituents — Snapshot (Before)',
        caption:
          'Top 30 tickers by weight before the regulatory event. Use this to sanity-check concentration, liquidity, and pre-event sensitivity.',
        files: [`charts/${p}_top30_before.html`, `tables/${p}_top30.html`],
      },
      {
        title: 'Market Structure — Clusters (Before)',
        caption:
          'Pre-announcement market structure. Clusters indicate correlated behavior that can amplify drawdowns.',
        files: [`charts/clusters_before.html`],
      },
    ]
  },
  AFTER: (risk) => {
    const p = RISK_TO_PREFIX[risk]
    return [
      {
        title: 'Top 30 Constituents — Rebalanced (After)',
        caption:
          'Post-event equity list after risk screening. Compare against Before to see rotation and de-risking effects.',
        files: [`charts/${p}_top30_after.html`],
      },
      {
        title: 'Sector Allocation — Post-Event',
        caption:
          'Sector tilt after applying legislative-risk adjustments. Expect defensives to rise for Low Exposure, cyclicals for High Exposure.',
        files: [`sector_dist/${p}_dist_full.html`, `sector_dist/${p}_dist_top30.html`],
      },
      {
        title: 'Market Structure — Clusters (After)',
        caption:
          'Groupings after the policy shock. Divergence vs. Before highlights names driving risk migration.',
        files: [`charts/clusters_after.html`],
      },
    ]
  },
}

function getReadingGuideForSrc(src) {
  const s = src.toLowerCase()
  if (s.includes('clusters_before')) {
    return {
      title: 'How to read this cluster map',
      bullets: [
        'Each point represents an S&P 500 stock positioned by its daily return on the announcement day (x-axis) and its 30-day volatility prior to the event (y-axis).',
        'This shows how stocks were distributed in terms of risk (volatility) and market reaction (return) before the law was announced.',
        'Clusters highlight groups of securities with similar behavior, separating highly volatile, reactive names from stable, low-risk performers.',
      ],
    }
  }
  if (s.includes('clusters_after')) {
    return {
      title: 'How to read this cluster map',
      bullets: [
        'Each point represents an S&P 500 stock using the same axes: announcement-day return (x) and 30-day pre-event volatility (y).',
        'Compare shapes and densities to the Before view to spot regime shifts and risk migration post-disclosure.',
        'Clusters that move up/right indicate pockets of elevated risk and outsized reactions; stable clusters remain low and near zero on the x-axis.',
      ],
    }
  }
  if (s.includes('_top30_before') && s.includes('charts/')) {
    return {
      title: 'How to read this scatter chart (Top 30 — Before)',
      bullets: [
        'Each point is one of the 30 most exposed S&P 500 stocks, positioned by announcement-day return (x) and 30-day volatility before the event (y).',
        'This focused view isolates names with the highest pre-announcement sensitivity to identify elevated risk or momentum.',
        'Clusters reveal collective behavior among key securities, distinguishing high-risk/high-reaction names from defensive, low-volatility patterns.',
      ],
    }
  }
  if (s.includes('_top30_after') && s.includes('charts/')) {
    return {
      title: 'How to read this scatter chart (Top 30 — After)',
      bullets: [
        'Points are the 30 most exposed stocks plotted by announcement-day return (x) and pre-event 30-day volatility (y) to preserve comparability with Before.',
        'Shifts relative to the Before view indicate which names absorbed or amplified the regulatory shock.',
        'Use dispersion and quadrant concentration to diagnose where risk concentrated after screening and rebalancing.',
      ],
    }
  }
  if (s.includes('sector_dist') && s.includes('_dist_full')) {
    return {
      title: 'How to read this bar chart (Full universe)',
      bullets: [
        "Each bar is portfolio exposure to a sector as a percentage of total holdings across the full S&P 500 universe considered in this study.",
        'Provides a broad market picture to contextualize concentration and diversification before and after adjustments.',
        'Use this with the Top 30 view to separate structural sector tilt from exposure driven by the most sensitive names.',
      ],
    }
  }
  if (s.includes('sector_dist') && s.includes('_dist_top30')) {
    return {
      title: 'How to read this bar chart (Top 30 focus)',
      bullets: [
        'Each bar reflects sector exposure within the 30 most exposed stocks — the names that drive most portfolio sensitivity to the regulation.',
        'Contrasting with the Full view highlights concentration risk and key contributors to regulatory impact.',
        'Large gaps vs. the Full view suggest targeted overexposure that may warrant position sizing or hedging.',
      ],
    }
  }
  if (s.includes('/tables/') && s.includes('_top30.html')) {
    return {
      title: 'How to read this table (Top 30 snapshot)',
      bullets: [
        'Rows list the 30 highest-exposure names with weights and diagnostics; use weights for concentration checks and liquidity for tradability.',
        'Cross-reference with the scatter to confirm whether heavy weights also sit in high-volatility, high-reaction quadrants.',
        'Look for sector clustering within the top rows as an early signal of thematic concentration.',
      ],
    }
  }
  return null
}

function ReadingGuide({ guide }) {
  if (!guide) return null
  return (
    <div className="mt-3 rounded-xl border border-gray-200 bg-gray-50 p-4">
      <div className="text-sm font-medium">{guide.title}</div>
      <ul className="mt-2 list-disc list-inside text-sm text-gray-700 space-y-1">
        {guide.bullets.map((b, i) => (
          <li key={i}>{b}</li>
        ))}
      </ul>
    </div>
  )
}

function IframeCard({ title, caption, files }) {
  const [errors, setErrors] = useState({})
  return (
    <div className="bg-white rounded-2xl border border-gray-200">
      <div className="p-5 border-b">
        <div className="font-medium">{title}</div>
        {caption && <p className="text-sm text-gray-600 mt-1">{caption}</p>}
      </div>
      <div className="grid gap-4 p-5">
        {files.map((src, i) => {
          const url = assetUrl(src)
          const guide = getReadingGuideForSrc(src)
          return (
            <div key={src + i} className="rounded-xl border overflow-hidden relative">
              <iframe
                title={src}
                src={url}
                className="w-full"
                style={{ height: 520 }}
                loading="lazy"
                onError={() => setErrors((e) => ({ ...e, [url]: true }))}
              />
              {errors[url] && (
                <div className="absolute inset-0 flex items-center justify-center bg-gray-50">
                  <div className="text-xs text-gray-600">
                    Couldn&apos;t load <span className="font-mono">{url}</span>. Confirm it exists under <span className="font-mono">public/{STATIC_PREFIX.replace(/^\//, '')}</span>.
                  </div>
                </div>
              )}
              <ReadingGuide guide={guide} />
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default function App() {
  const [risk, setRisk] = useState('MEDIUM')
  const [file, setFile] = useState(null)
  const [dragOver, setDragOver] = useState(false)
  const [jobId, setJobId] = useState(null)
  const [status, setStatus] = useState(null)
  const [resultUrl, setResultUrl] = useState(null)
  const [resultData, setResultData] = useState(null)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [progress, setProgress] = useState(0)
  const [showConfetti, setShowConfetti] = useState(false)
  const [viewport, setViewport] = useState({ width: 0, height: 0 })
  const [view, setView] = useState('HOME')

  const timersRef = useRef([])
  const intervalsRef = useRef([])
  const dollarImgRef = useRef(null)
  const coinImgRef = useRef(null)

  useEffect(() => {
    const onResize = () => setViewport({ width: window.innerWidth, height: window.innerHeight })
    onResize()
    window.addEventListener('resize', onResize)
    return () => {
      window.removeEventListener('resize', onResize)
      timersRef.current.forEach((t) => clearTimeout(t))
      intervalsRef.current.forEach((i) => clearInterval(i))
      timersRef.current = []
      intervalsRef.current = []
    }
  }, [])

  useEffect(() => {
    const billSvg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 80"><rect width="160" height="80" rx="10" fill="#22c55e"/><rect x="10" y="10" width="140" height="60" rx="8" fill="#16a34a"/><circle cx="80" cy="40" r="18" fill="#ffffff" fill-opacity="0.9"/><text x="80" y="47" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="28" font-weight="700" fill="#111827">$</text></svg>`
    const coinSvg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 96 96"><circle cx="48" cy="48" r="44" fill="#facc15"/><circle cx="48" cy="48" r="36" fill="#fde047"/><text x="48" y="58" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="36" font-weight="700" fill="#92400e">$</text></svg>`
    const bill = new Image()
    const coin = new Image()
    bill.src = 'data:image/svg+xml;utf8,' + encodeURIComponent(billSvg)
    coin.src = 'data:image/svg+xml;utf8,' + encodeURIComponent(coinSvg)
    dollarImgRef.current = bill
    coinImgRef.current = coin
  }, [])

  function pick(f) {
    if (!isAllowed(f)) {
      setError('Only .html or .xml files are accepted.')
      setFile(null)
      return
    }
    setError(null)
    setFile(f)
  }

  function onPick(e) {
    pick(e.target.files?.[0])
  }

  function onDrop(e) {
    e.preventDefault()
    setDragOver(false)
    pick(e.dataTransfer.files?.[0])
  }

  function clearTimers() {
    timersRef.current.forEach((t) => clearTimeout(t))
    intervalsRef.current.forEach((i) => clearInterval(i))
    timersRef.current = []
    intervalsRef.current = []
  }

  async function start() {
    setError(null)
    setResultUrl(null)
    setResultData(null)
    setShowConfetti(false)
    setProgress(0)
    if (!file) {
      setError('Please select a .html or .xml file.')
      return
    }
    try {
      setBusy(true)
      setStatus('PENDING')
      const jid = `JOB-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
      setJobId(jid)
      const toRunning = setTimeout(() => setStatus('RUNNING'), 900)
      timersRef.current.push(toRunning)
      const totalMs = 9000
      const tickMs = 120
      const ticks = Math.floor((totalMs - 900) / tickMs)
      let k = 0
      const interval = setInterval(() => {
        k += 1
        const pct = Math.min(96, Math.round((k / ticks) * 96))
        setProgress(pct)
      }, tickMs)
      intervalsRef.current.push(interval)
      const toComplete = setTimeout(() => {
        intervalsRef.current.forEach((i) => clearInterval(i))
        intervalsRef.current = []
        const mock = {
          summary:
            'Document parsed successfully. Portfolio screened for legislative risk; sector tilts and top holdings updated.',
          stocks: ['AAPL', 'MSFT', 'AMZN'],
          comment: `Processed ${file.name} under ${risk} profile. Use “View After Analysis” to see the recommended positioning and rationale.`,
        }
        setProgress(100)
        setResultData(mock)
        const blob = new Blob([JSON.stringify(mock, null, 2)], { type: 'application/json' })
        const url = URL.createObjectURL(blob)
        setResultUrl(url)
        setStatus('COMPLETED')
        setBusy(false)
        setShowConfetti(true)
        const confettiTimer = setTimeout(() => setShowConfetti(false), 2500)
        timersRef.current.push(confettiTimer)
      }, totalMs)
      timersRef.current.push(toComplete)
    } catch (e) {
      setError(e.message || 'Unexpected error')
      setBusy(false)
      setStatus('FAILED')
    }
  }

  function cancelPolling() {
    clearTimers()
    setBusy(false)
  }

  const current = PROFILES.find((p) => p.key === risk)

  return (
    <div className="min-h-screen bg-gray-50">
      <AnimatePresence>
        {showConfetti && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="fixed inset-0 pointer-events-none z-50">
            <Confetti
              width={viewport.width}
              height={viewport.height}
              numberOfPieces={320}
              gravity={0.4}
              recycle={false}
              drawShape={(ctx) => {
                const imgs = [dollarImgRef.current, coinImgRef.current].filter(Boolean)
                if (!imgs.length) return
                const img = imgs[Math.random() < 0.65 ? 0 : 1]
                if (!img || !img.complete) return
                const s = 28 + Math.random() * 28
                const aspect = img.naturalHeight / img.naturalWidth
                ctx.drawImage(img, -s / 2, -(s * aspect) / 2, s, s * aspect)
              }}
            />
          </motion.div>
        )}
      </AnimatePresence>

      <header className="border-b bg-white">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
          <h1 className="text-lg font-semibold">LegImpact | The AI Legislative Risk Assistant</h1>
          <span className="text-xs text-gray-500">DATATHON 2025</span>
        </div>
      </header>

      {view === 'HOME' && (
        <main className="max-w-6xl mx-auto px-6 py-8 grid grid-cols-1 lg:grid-cols-3 gap-6">
          <section className="lg:col-span-2 space-y-6">
            <div className="bg-white rounded-2xl border border-gray-200 p-6">
              <div className="flex items-center justify-between">
                <h2 className="font-medium">Choose Portfolio</h2>
                {busy && <span className="w-3 h-3 border-2 border-gray-300 border-t-gray-900 rounded-full animate-spin" />}
              </div>
              <div className="mt-4 flex flex-wrap gap-2">
                {PROFILES.map((p) => (
                  <button
                    key={p.key}
                    disabled={busy}
                    onClick={() => setRisk(p.key)}
                    className={['px-4 py-2 rounded-xl border transition', risk === p.key ? 'bg-gray-900 text-white border-gray-900' : 'bg-white hover:bg-gray-100 border-gray-200'].join(' ')}
                  >
                    {p.key}
                  </button>
                ))}
              </div>
              <div className="mt-4 rounded-xl border border-gray-200 p-4 bg-gray-50">
                <div className="font-medium">{current?.title}</div>
                <p className="text-sm text-gray-700 mt-1">{current?.intro}</p>
                {!!current?.bullets?.length && (
                  <ul className="mt-3 list-disc list-inside text-sm text-gray-800 space-y-1">
                    {current.bullets.map((b, i) => (
                      <li key={i}>{b}</li>
                    ))}
                  </ul>
                )}
              </div>
            </div>

            <div className="bg-white rounded-2xl border border-gray-200 p-6">
              <h2 className="font-medium">Upload law document (.html or .xml)</h2>
              <div
                onDragOver={(e) => {
                  e.preventDefault()
                  setDragOver(true)
                }}
                onDragLeave={() => setDragOver(false)}
                onDrop={onDrop}
                className={['mt-4 rounded-xl border-2 border-dashed p-6 text-center transition', dragOver ? 'border-gray-900 bg-gray-50' : 'border-gray-300 bg-white'].join(' ')}
              >
                <p className="text-sm text-gray-600">
                  Drag a file here, or{' '}
                  <label className="text-gray-900 underline cursor-pointer">
                    browse
                    <input type="file" className="hidden" onChange={onPick} disabled={busy} accept=".html,.xml" />
                  </label>
                </p>
                {file && (
                  <div className="mt-3 text-sm">
                    <span className="font-medium">{file.name}</span>{' '}
                    <span className="text-gray-500">({Math.ceil(file.size / 1024)} kB)</span>
                  </div>
                )}
              </div>
              <div className="mt-4 flex flex-wrap items-center gap-3">
                <button onClick={() => setView('BEFORE')} disabled={busy} className="px-4 py-2 rounded-xl border border-gray-300 hover:bg-gray-50">
                  View Before Analysis
                </button>
                <button onClick={start} disabled={busy || !file} className="px-4 py-2 rounded-xl bg-gray-900 text-white hover:bg-gray-800 disabled:opacity-60">
                  Run Analysis
                </button>
                {busy && (
                  <button onClick={cancelPolling} className="px-3 py-2 rounded-xl border border-gray-300 hover:bg-gray-50">Cancel</button>
                )}
                {error && <div className="text-sm text-rose-600">{error}</div>}
              </div>
            </div>
          </section>

          <section className="space-y-6">
            <div className="bg-white rounded-2xl border border-gray-200 p-6 space-y-4">
              <h2 className="font-medium">Portfolio Analysis</h2>
              <div className="text-sm grid grid-cols-3 gap-y-2">
                <div className="text-gray-500">Risk</div>
                <div className="col-span-2 font-medium">{risk}</div>
                <div className="text-gray-500">Job ID</div>
                <div className="col-span-2 font-mono break-all">{jobId || '—'}</div>
                <div className="text-gray-500">Status</div>
                <div className="col-span-2">
                  <span className={['px-2 py-0.5 rounded-full border text-xs', status === 'COMPLETED' ? 'bg-emerald-50 border-emerald-200' : status === 'FAILED' ? 'bg-rose-50 border-rose-200' : 'bg-white border-gray-200'].join(' ')}>
                    {status || '—'}
                  </span>
                </div>
              </div>

              <AnimatePresence>
                {status === 'RUNNING' && (
                  <motion.div initial={{ opacity: 0, y: -6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -6 }} className="rounded-xl border border-gray-200 p-4 bg-gray-50">
                    <div className="text-sm font-medium mb-2">Analyzing document…</div>
                    <div className="w-full h-2 bg-white border border-gray-200 rounded-full overflow-hidden">
                      <motion.div className="h-full bg-gray-900" initial={{ width: '0%' }} animate={{ width: `${progress}%` }} transition={{ type: 'tween', ease: 'easeOut', duration: 0.12 }} />
                    </div>
                    <div className="mt-2 text-xs text-gray-600">{progress}%</div>
                  </motion.div>
                )}
              </AnimatePresence>

              {resultUrl && (
                <a className="inline-flex items-center gap-2 text-sm text-blue-600 underline" href={resultUrl} target="_blank" rel="noreferrer">
                  Download JSON summary
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 7h8m0 0v8m0-8L8 15" />
                  </svg>
                </a>
              )}

              {status === 'COMPLETED' && resultData && (
                <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} className="mt-3 rounded-xl border border-gray-200 p-4 bg-gray-50 space-y-3">
                  {resultData.summary && <div className="text-sm"><span className="font-medium">Summary:</span> {resultData.summary}</div>}
                  {Array.isArray(resultData.stocks) && resultData.stocks.length > 0 && (
                    <div>
                      <div className="text-sm font-medium mb-1">Stocks (demo):</div>
                      <ul className="list-disc list-inside text-sm text-gray-800">
                        {resultData.stocks.map((s) => (
                          <li key={s} className="font-mono">
                            {s}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                  {resultData.comment && <div className="text-sm text-gray-800"><span className="font-medium">Comment:</span> {resultData.comment}</div>}
                  <div className="flex gap-2">
                    <button onClick={() => setView('AFTER')} className="mt-2 px-4 py-2 rounded-xl bg-gray-900 text-white hover:bg-gray-800">View After Analysis</button>
                    <button onClick={() => setView('BEFORE')} className="mt-2 px-4 py-2 rounded-xl border border-gray-300 hover:bg-gray-50">View Before</button>
                  </div>
                </motion.div>
              )}
            </div>
          </section>
        </main>
      )}

      {view === 'BEFORE' && (
        <main className="max-w-6xl mx-auto px-6 py-8 space-y-6">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-lg font-semibold">Before Analysis — {PROFILES.find((p) => p.key === risk)?.title}</div>
              <div className="text-sm text-gray-600 mt-1">Exploratory views to understand current exposure before applying legislative-risk adjustments.</div>
            </div>
            <div className="flex gap-2">
              <button onClick={() => setView('HOME')} className="px-4 py-2 rounded-xl border border-gray-300 hover:bg-gray-50">Back</button>
            </div>
          </div>
          {ASSETS.BEFORE(risk).map((b) => (
            <IframeCard key={b.title} title={b.title} caption={b.caption} files={b.files} />
          ))}
        </main>
      )}

      {view === 'AFTER' && (
        <main className="max-w-6xl mx-auto px-6 py-8 space-y-6">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-lg font-semibold">After Analysis — {PROFILES.find((p) => p.key === risk)?.title}</div>
              <div className="text-sm text-gray-600 mt-1">Recommended positioning and diagnostics after screening for legislative risk.</div>
            </div>
            <div className="flex gap-2">
              <button onClick={() => setView('HOME')} className="px-4 py-2 rounded-xl border border-gray-300 hover:bg-gray-50">Back</button>
              <button onClick={() => setView('BEFORE')} className="px-4 py-2 rounded-xl border border-gray-300 hover:bg-gray-50">View Before</button>
            </div>
          </div>
          {ASSETS.AFTER(risk).map((a) => (
            <IframeCard key={a.title} title={a.title} caption={a.caption} files={a.files} />
          ))}
        </main>
      )}

      <footer className="py-6 text-center text-xs text-gray-500">AWS S3 · API Gateway · Lambda · SageMaker Async</footer>
    </div>
  )
}
