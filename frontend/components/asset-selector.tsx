"use client"

import { useMemo, useState, useRef, useEffect } from "react"
import { ChevronDown, Search, BarChart3, type LucideIcon } from "lucide-react"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Empty, EmptyContent, EmptyDescription, EmptyTitle } from "@/components/ui/empty"
import type { Asset } from "@/hooks/use-quotex-ws"
import {
  matchAllowedMarket,
} from "@/lib/allowed-markets"
import { MarketFlagFor, marketCategory } from "@/components/signal/market-flag"

const CATEGORY_TABS: Array<{ id: string; label: string }> = [
  { id: "currencies", label: "Currencies" },
  { id: "crypto", label: "Crypto" },
  { id: "commodities", label: "Commodities" },
  { id: "stocks", label: "Stocks" },
  { id: "indices", label: "Indices" },
]

type Props = {
  assets: Asset[]
  current: string | null
  onSelect: (symbol: string) => void
  loading?: boolean
  /** Custom icon for the market selector trigger. Defaults to BarChart3. */
  icon?: LucideIcon
  /**
   * Disable the trigger button (e.g. when another tab in the same
   * browser already owns the per-user pair lock). Visually softens
   * the trigger and prevents the dropdown from opening.
   */
  disabled?: boolean
}

export function AssetSelector({ assets, current, onSelect, loading, icon: Icon = BarChart3, disabled = false }: Props) {
  const [query, setQuery] = useState("")
  const [open, setOpen] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)

  // Auto-focus search input when dropdown opens
  useEffect(() => {
    if (open) {
      // Small delay to ensure DOM is ready
      const timer = setTimeout(() => {
        inputRef.current?.focus()
      }, 50)
      return () => clearTimeout(timer)
    } else {
      // Clear search when dropdown closes
      setQuery("")
    }
  }, [open])

  // Every market the backend sends is live at Quotex right now; curated
  // markets keep their friendly label and product ordering, the rest are
  // appended with the broker's own name.
  //
  // Each row also gets a precomputed ``haystack`` string used by the
  // search filter below: the broker symbol, the broker name, AND the
  // friendly label, all normalised (lowercased + punctuation stripped)
  // and concatenated. Doing this once per asset list is much cheaper
  // than re-normalising three strings on every keystroke.
  const allowedAssets = useMemo(() => {
    const decorated: Array<{
      asset: Asset
      category: string
      displayLabel: string
      haystack: string
    }> = []
    const seenLabels = new Set<string>()
    for (const a of assets) {
      // Every market the backend lists is live at Quotex. Curated markets
      // get their friendly label; the rest use the broker name.
      const entry = matchAllowedMarket(a.symbol, a.name)
      const label = entry?.label ?? (a.name || a.symbol)
      if (seenLabels.has(label)) continue
      seenLabels.add(label)
      decorated.push({
        asset: a,
        category: marketCategory(a.symbol, a.name, a.category),
        displayLabel: label,
        haystack: buildHaystack(a.symbol, a.name, label),
      })
    }
    // Same order as Quotex: highest payout first.
    decorated.sort(
      (a, b) =>
        (b.asset.payout ?? -1) - (a.asset.payout ?? -1) ||
        a.displayLabel.localeCompare(b.displayLabel),
    )
    return decorated
  }, [assets])

  const tabs = useMemo(
    () => CATEGORY_TABS.filter((t) => allowedAssets.some((r) => r.category === t.id)),
    [allowedAssets],
  )
  const [tab, setTab] = useState<string>("currencies")
  const currentCategory = allowedAssets.find((r) => r.asset.symbol === current)?.category
  useEffect(() => {
    if (open && currentCategory) setTab(currentCategory)
  }, [open, currentCategory])
  const activeTab = tabs.some((t) => t.id === tab) ? tab : tabs[0]?.id ?? "currencies"

  // Tokenised, punctuation-insensitive search.
  //
  // The previous implementation `.includes(q)` against three raw
  // strings, which silently failed on totally reasonable queries:
  //
  //   - "usd brl"   ❌ (haystack had "usd/brl", `/` broke `includes`)
  //   - "EURUSD"    ❌ (label rendered as "eur/usd")
  //   - "btc usd"   ❌ (only the symbol matched, query had a space)
  //
  // Now we normalise both sides identically — strip everything that
  // isn't a letter or digit — then split the query on whitespace and
  // require every token to substring-match the haystack. That makes
  // "usd brl", "USDBRL", "usd/brl", "BRL usd", and "USD BRL otc" all
  // return USD/BRL OTC, which is what users actually expect.
  const filtered = useMemo(() => {
    const tokens = normaliseQuery(query)
    // Searching looks across every group; otherwise show the active tab.
    if (tokens.length === 0) return allowedAssets.filter((r) => r.category === activeTab)
    return allowedAssets.filter(({ haystack }) =>
      tokens.every((t) => haystack.includes(t)),
    )
  }, [allowedAssets, query, activeTab])

  const open_only = filtered.filter(({ asset }) => asset.is_open !== false)
  const closed_only = filtered.filter(({ asset }) => asset.is_open === false)

  // Find the active row in the *filtered* list so the trigger can
  // surface the friendly label (e.g. "EUR/USD (OTC)") instead of the
  // raw broker symbol. If the current selection somehow falls outside
  // the allow-list (legacy URL, etc.) we fall back to the broker name
  // so the user isn't left with an empty trigger.
  const currentRow = allowedAssets.find(
    ({ asset }) => asset.symbol === current,
  )
  const currentAsset = currentRow?.asset ?? assets.find((a) => a.symbol === current)
  const currentLabel =
    current === null
      ? "Select Market"
      : currentRow?.displayLabel ?? currentAsset?.symbol ?? "Select Market"
  const hasSelection = current !== null

  return (
    <DropdownMenu open={disabled ? false : open} onOpenChange={(v) => !disabled && setOpen(v)}>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          disabled={disabled}
          className={`inline-flex min-w-32 items-center justify-between gap-1.5 rounded-lg border px-2 py-1.5 font-mono text-[11px] shadow-sm transition-colors focus:outline-none focus:ring-2 focus:ring-[#5BC0D8]/50 disabled:cursor-not-allowed disabled:opacity-55 sm:min-w-48 sm:gap-2 sm:px-3 sm:py-2 sm:text-sm ${
            hasSelection
              ? "border-[#1B7892]/50 bg-[#03171E]/80 text-[#7DE3FF] hover:border-[#5BC0D8]/70 hover:bg-[#03171E]"
              : "border-[#5BC0D8]/60 bg-[#1B7892]/20 text-[#7DE3FF] hover:border-[#5BC0D8]/80 hover:bg-[#1B7892]/30"
          }`}
        >
          <span className="flex items-center gap-1.5 truncate sm:gap-2">
            {hasSelection ? (
              <MarketFlagFor
                market={currentAsset?.symbol ?? null}
                alt={currentRow?.displayLabel}
                size="xs"
              />
            ) : (
              <Icon className="size-3.5 sm:size-4 text-[#7DE3FF]" aria-hidden />
            )}
            <span className={`truncate ${hasSelection ? "text-[#E8F4F7]" : "text-[#7DE3FF] italic"}`}>{currentLabel}</span>
          </span>
          <span className="flex items-center gap-1 sm:gap-2">
            {hasSelection && currentAsset?.payout && (
              <span className="hidden text-[10px] text-[#7DE3FF] sm:inline sm:text-xs">{currentAsset.payout}%</span>
            )}
            <ChevronDown className={`size-3.5 sm:size-4 ${hasSelection ? "text-[#5BC0D8]" : "text-[#7DE3FF]"}`} aria-hidden />
          </span>
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-72 max-h-80 overflow-hidden flex flex-col p-0 bg-[#03171E] border-[#1B7892]/50 sm:w-80 sm:max-h-96">
        <div className="p-1.5 border-b border-[#1B7892]/30 sm:p-2">
          <div className="relative flex items-center">
            <Search className="absolute left-2.5 size-3.5 text-[#5BC0D8]/70 pointer-events-none sm:left-3 sm:size-4" aria-hidden />
            <input
              ref={inputRef}
              type="text"
              inputMode="search"
              autoComplete="off"
              autoCorrect="off"
              autoCapitalize="off"
              spellCheck={false}
              placeholder="Search markets..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                // Prevent dropdown keyboard navigation from interfering with typing
                e.stopPropagation()
              }}
              className="w-full rounded-md border border-[#1B7892]/40 bg-[#02141A]/80 py-1.5 pl-8 pr-2.5 text-[12px] text-[#E8F4F7] placeholder:text-[#5BC0D8]/50 outline-none focus:border-[#5BC0D8]/70 focus:ring-2 focus:ring-[#5BC0D8]/30 sm:py-2 sm:pl-9 sm:pr-3 sm:text-sm"
            />
          </div>
          {tabs.length > 0 && (
            <div className="mt-1.5 flex gap-1 overflow-x-auto pb-2 [scrollbar-width:thin] sm:mt-2" role="tablist" data-testid="market-category-tabs">
              {tabs.map((t) => {
                const selected = t.id === activeTab && !query
                const count = allowedAssets.filter((r) => r.category === t.id).length
                return (
                  <button
                    key={t.id}
                    type="button"
                    role="tab"
                    aria-selected={selected}
                    data-testid={`market-tab-${t.id}`}
                    onClick={() => {
                      setTab(t.id)
                      setQuery("")
                    }}
                    className={`shrink-0 rounded-md px-2 py-1 text-[10px] font-medium transition-colors sm:text-xs ${
                      selected
                        ? "bg-[#1B7892]/50 text-[#E8F4F7]"
                        : "text-[#7DE3FF]/70 hover:bg-[#1B7892]/20 hover:text-[#E8F4F7]"
                    }`}
                  >
                    {t.label} <span className="text-[#5BC0D8]/60">{count}</span>
                  </button>
                )
              })}
            </div>
          )}
        </div>

        <div className="overflow-y-auto flex-1">
          {loading && (
            <div className="px-3 py-4 text-[12px] text-[#7DE3FF]/70 text-center sm:py-6 sm:text-sm">Loading assets...</div>
          )}
          {!loading && filtered.length === 0 && (
            <Empty className="py-6 sm:py-8">
              <EmptyContent>
                <EmptyTitle className="text-[#E8F4F7] text-sm">No assets match</EmptyTitle>
                <EmptyDescription className="text-[#7DE3FF]/60 text-xs">Try a different search term.</EmptyDescription>
              </EmptyContent>
            </Empty>
          )}

          {open_only.length > 0 && (
            <>
              <DropdownMenuLabel className="text-[10px] uppercase tracking-wide text-[#5BC0D8]/70 sm:text-xs">
                {query ? "Results" : CATEGORY_TABS.find((t) => t.id === activeTab)?.label ?? "Open"} · Payout high → low
              </DropdownMenuLabel>
              {open_only.map(({ asset, displayLabel }) => (
                <AssetRow
                  key={asset.symbol}
                  asset={asset}
                  displayLabel={displayLabel}
                  active={asset.symbol === current}
                  onClick={() => {
                    onSelect(asset.symbol)
                    setOpen(false)
                  }}
                />
              ))}
            </>
          )}
          {closed_only.length > 0 && (
            <>
              <DropdownMenuSeparator className="bg-[#1B7892]/30" />
              <DropdownMenuLabel className="text-[10px] uppercase tracking-wide text-[#5BC0D8]/70 sm:text-xs">
                Closed
              </DropdownMenuLabel>
              {closed_only.map(({ asset, displayLabel }) => (
                <AssetRow
                  key={asset.symbol}
                  asset={asset}
                  displayLabel={displayLabel}
                  active={asset.symbol === current}
                  onClick={() => {
                    onSelect(asset.symbol)
                    setOpen(false)
                  }}
                />
              ))}
            </>
          )}
        </div>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

/**
 * Strip everything that's not a letter or a digit and lowercase the
 * result. Used by both the haystack builder and the query normaliser
 * so any character of punctuation in the user's query — ``/``, ``_``,
 * ``-``, ``.``, ``(``, ``)`` — silently cancels itself out.
 */
function normaliseAlphaNum(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, "")
}

/**
 * Build the searchable haystack for one asset row. Concatenates the
 * symbol, the broker-supplied name, and the curated label after
 * normalisation, separated by spaces so substring searches don't
 * spuriously bridge fields (e.g. searching "usdbrl" should match
 * within ``displayLabel`` rather than across ``symbol|name``).
 */
function buildHaystack(symbol: string, name: string | undefined | null, label: string): string {
  return [symbol, name ?? "", label]
    .map(normaliseAlphaNum)
    .filter(Boolean)
    .join(" ")
}

/**
 * Split the query on whitespace, normalise each token, drop empties.
 * Returning ``[]`` signals "no filter" to the caller.
 */
function normaliseQuery(q: string): string[] {
  return q
    .split(/\s+/)
    .map(normaliseAlphaNum)
    .filter(Boolean)
}

function AssetRow({
  asset,
  displayLabel,
  active,
  onClick,
}: {
  asset: Asset
  /** Curated label from the allow-list (e.g. ``"EUR/USD (OTC)"``). */
  displayLabel: string
  active: boolean
  onClick: () => void
}) {
  return (
    <DropdownMenuItem
      onSelect={(e) => {
        e.preventDefault()
        onClick()
      }}
      className={`font-mono flex items-center justify-between gap-2 cursor-pointer focus:bg-[#1B7892]/40 focus:text-[#7DE3FF] px-2 py-1.5 sm:gap-3 sm:px-3 sm:py-2 ${
        active 
          ? "bg-[#1B7892]/30 text-[#7DE3FF]" 
          : "text-[#E8F4F7] hover:bg-[#1B7892]/20 hover:text-[#E8F4F7]"
      }`}
      data-active={active || undefined}
    >
      <div className="flex items-center gap-2 min-w-0 sm:gap-2.5">
        <MarketFlagFor market={asset.symbol} alt={displayLabel} size="xs" />
        <div className="flex flex-col min-w-0">
          <span className="truncate text-[11px] sm:text-sm">{displayLabel}</span>
          <span className="text-[10px] text-[#5BC0D8]/60 truncate sm:text-xs">
            {asset.symbol}
          </span>
        </div>
      </div>
      <div className="flex items-center gap-1.5 shrink-0 sm:gap-2">
        {asset.payout != null && (
          <span className="text-[10px] text-[#7DE3FF] sm:text-xs">{asset.payout}%</span>
        )}
        <span
          className={`size-1.5 rounded-full sm:size-2 ${
            asset.is_open === false ? "bg-rose-400" : "bg-emerald-400"
          }`}
          aria-hidden
        />
      </div>
    </DropdownMenuItem>
  )
}
