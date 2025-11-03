import React, { useEffect, useRef, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE
const PROFILES = [
  { key: 'SAFE',   title: 'Safe Portfolio',    text: 'Capital preservation and steady income via gov/IG bonds and cash. Lower volatility and returns.' },
  { key: 'MEDIUM', title: 'Balanced Portfolio',text: 'Blend of equities and fixed income for moderated risk with upside participation.' },
  { key: 'RISKY',  title: 'Growth Portfolio',  text: 'Equity-tilted for higher long-term growth and higher short-term volatility.' },
]
const isAllowed = (f) => !!f && /\.(html?|xml)$/i.test(f.name)

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
  const pollRef = useRef(null)

  useEffect(() => {
    return () => { clearInterval(pollRef.current) }
  }, [])

  function pick(f) {
    if (!isAllowed(f)) { setError('Only .html or .xml files are accepted.'); setFile(null); return }
    setError(null); setFile(f)
  }
  function onPick(e)  { pick(e.target.files?.[0]) }
  function onDrop(e)  { e.preventDefault(); setDragOver(false); pick(e.dataTransfer.files?.[0]) }

  async function getJob(id) {
    const r = await fetch(`${API_BASE}/jobs/${id}`, {
      headers: { 'Accept': 'application/json' },
      cache: 'no-store',
    })
    if (r.status === 404) return null
    if (!r.ok) throw new Error('Status check failed')
    const d = await r.json()
    setStatus(d.status)
    if (d.result_url) setResultUrl(d.result_url)
    return d
  }

  async function start() {
    setError(null); setResultUrl(null)
    if (!file) { setError('Please select a .html or .xml file.'); return }
    try {
      setBusy(true)
      const contentType = file.type || (/\.(xml)$/i.test(file.name) ? 'application/xml' : 'text/html')
      const res = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ risk_profile: risk, content_type: contentType, filename: file.name }),
      })
      if (!res.ok) throw new Error('Failed to create job')
      const { job_id, upload_url } = await res.json()
      setJobId(job_id); setStatus('PENDING')

      const put = await fetch(upload_url, {
        method: 'PUT',
        headers: { 'Content-Type': contentType },
        body: file,
      })
      if (!put.ok) {
        const text = await put.text().catch(() => '')
        throw new Error(`Upload failed (${put.status}) ${text}`)
      }

      await new Promise(r => setTimeout(r, 800))
      const startRes = await fetch(`${API_BASE}/jobs/${job_id}/start`, { method: 'POST' })
      if (startRes.status !== 202) throw new Error('Failed to start job')
      setStatus('RUNNING')
      beginPolling(job_id)
    } catch (e) {
      console.error(e)
      setError(e.message || 'Unexpected error')
      setBusy(false)
      setStatus('FAILED')
    }
  }

  function beginPolling(id) {
    clearInterval(pollRef.current)
    getJob(id).catch(() => {})
    pollRef.current = setInterval(async () => {
      try {
        const data = await getJob(id)
        if (!data) { clearInterval(pollRef.current); setBusy(false); return }
        if (data.result_url || data.status === 'FAILED') {
          clearInterval(pollRef.current)
          setBusy(false)
        }
      } catch {}
    }, 2500)
  }

  function cancelPolling() { clearInterval(pollRef.current); setBusy(false) }

  useEffect(() => {
    let aborted = false
    async function fetchResult(u) {
      try {
        const r = await fetch(u, { cache: 'no-store' })
        if (!r.ok) throw new Error('Failed to load result')
        const d = await r.json()
        if (!aborted) setResultData(d)
      } catch (e) {
        console.error(e)
      }
    }
    if (resultUrl) { setResultData(null); fetchResult(resultUrl) }
    return () => { aborted = true }
  }, [resultUrl])

  const current = PROFILES.find((p) => p.key === risk)

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="border-b bg-white">
        <div className="max-w-5xl mx-auto px-6 py-4 flex items-center justify-between">
          <h1 className="text-lg font-semibold">Portfolio & Law Docs</h1>
          <span className="text-xs text-gray-500">SageMaker Async · S3 · API Gateway</span>
        </div>
      </header>

      <main className="max-w-5xl mx-auto px-6 py-8 grid grid-cols-1 lg:grid-cols-3 gap-6">
        <section className="lg:col-span-2 space-y-6">
          <div className="bg-white rounded-2xl border border-gray-200 p-6">
            <div className="flex items-center justify-between">
              <h2 className="font-medium">Risk profile</h2>
              {busy && <span className="w-3 h-3 border-2 border-gray-300 border-t-gray-900 rounded-full animate-spin" />}
            </div>
            <div className="mt-4 flex gap-2">
              {PROFILES.map((p) => (
                <button
                  key={p.key}
                  disabled={busy}
                  onClick={() => setRisk(p.key)}
                  className={[
                    'px-4 py-2 rounded-xl border transition',
                    risk === p.key ? 'bg-gray-900 text-white border-gray-900' : 'bg-white hover:bg-gray-100 border-gray-200',
                  ].join(' ')}
                >
                  {p.key}
                </button>
              ))}
            </div>
            <div className="mt-4 rounded-xl border border-gray-200 p-4 bg-gray-50">
              <div className="font-medium">{current?.title}</div>
              <p className="text-sm text-gray-700 mt-1">{current?.text}</p>
            </div>
          </div>

          <div className="bg-white rounded-2xl border border-gray-200 p-6">
            <h2 className="font-medium">Upload law document (.html or .xml)</h2>
            <div
              onDragOver={(e) => { e.preventDefault(); setDragOver(true) }}
              onDragLeave={() => setDragOver(false)}
              onDrop={onDrop}
              className={[
                'mt-4 rounded-xl border-2 border-dashed p-6 text-center transition',
                dragOver ? 'border-gray-900 bg-gray-50' : 'border-gray-300 bg-white',
              ].join(' ')}
            >
              <p className="text-sm text-gray-600">
                Drag a file here, or{' '}
                <label className="text-gray-900 underline cursor-pointer">
                  browse
                  <input
                    type="file"
                    className="hidden"
                    onChange={onPick}
                    disabled={busy}
                    accept=".html,.xml"
                  />
                </label>
              </p>
              {file && (
                <div className="mt-3 text-sm">
                  <span className="font-medium">{file.name}</span>{' '}
                  <span className="text-gray-500">({Math.ceil(file.size / 1024)} kB)</span>
                </div>
              )}
            </div>

            <div className="mt-4 flex items-center gap-3">
              <button onClick={start} disabled={busy || !file} className="px-4 py-2 rounded-xl bg-gray-900 text-white hover:bg-gray-800 disabled:opacity-60">
                Start Processing
              </button>
              {busy && (
                <button onClick={cancelPolling} className="px-3 py-2 rounded-xl border border-gray-300 hover:bg-gray-50">
                  Cancel
                </button>
              )}
              {error && <div className="text-sm text-rose-600">{error}</div>}
            </div>
          </div>
        </section>

        <section className="space-y-6">
          <div className="bg-white rounded-2xl border border-gray-200 p-6 space-y-4">
            <h2 className="font-medium">Job</h2>
            <div className="text-sm grid grid-cols-3 gap-y-2">
              <div className="text-gray-500">Risk</div><div className="col-span-2 font-medium">{risk}</div>
              <div className="text-gray-500">Job ID</div><div className="col-span-2 font-mono break-all">{jobId || '—'}</div>
              <div className="text-gray-500">Status</div>
              <div className="col-span-2">
                <span className={[
                  'px-2 py-0.5 rounded-full border text-xs',
                  status === 'COMPLETED' ? 'bg-emerald-50 border-emerald-200'
                  : status === 'FAILED' ? 'bg-rose-50 border-rose-200'
                  : 'bg-white border-gray-200',
                ].join(' ')}>
                  {status || '—'}
                </span>
              </div>
            </div>

            {resultUrl && (
              <a className="inline-flex items-center gap-2 text-sm text-blue-600 underline" href={resultUrl} target="_blank" rel="noreferrer">
                Download result
                <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 7h8m0 0v8m0-8L8 15"/>
                </svg>
              </a>
            )}

            {status === 'COMPLETED' && resultData && (
              <div className="mt-3 rounded-xl border border-gray-200 p-4 bg-gray-50 space-y-3">
                {resultData.summary && (
                  <div className="text-sm"><span className="font-medium">Summary:</span> {resultData.summary}</div>
                )}
                {Array.isArray(resultData.stocks) && resultData.stocks.length > 0 && (
                  <div>
                    <div className="text-sm font-medium mb-1">Stocks (demo):</div>
                    <ul className="list-disc list-inside text-sm text-gray-800">
                      {resultData.stocks.map((s) => (<li key={s} className="font-mono">{s}</li>))}
                    </ul>
                  </div>
                )}
                {resultData.comment && (
                  <div className="text-sm text-gray-800">
                    <span className="font-medium">Comment:</span> {resultData.comment}
                  </div>
                )}
              </div>
            )}

            <div className="text-xs text-gray-500 pt-2 border-t">
              Calls <span className="font-mono">POST /jobs</span>, uploads to S3, then
              <span className="font-mono"> POST /jobs/{'{' }id{'}'}/start</span>. Polls
              <span className="font-mono"> GET /jobs/{'{' }id{'}'}</span>.
            </div>
          </div>
        </section>
      </main>

      <footer className="py-6 text-center text-xs text-gray-500">AWS S3 · API Gateway · Lambda · SageMaker Async</footer>
    </div>
  )
}
