⟨FromTS⇐FromTimestamp, ToTS⇐ToTimestamp⟩ ← •Import "bqn-libs/datetime.bqn"
⟨JParse⇐Parse⟩ ← •Import "bqn-libs/json.bqn"
csv←•Import "bqn-libs/csv.bqn"
str←•Import "bqn-libs/strings.bqn"
ToUpper ← -⟜(32×1="a{"⊸⍋)
RowNu ← { ((↕≠𝕩)-𝕨) ∾˘ 𝕩}
SplitNum ←•Parsefloat‿⊢{𝕎 𝕩}¨(("-."∾'0' + ↕10) (((⊢⋈¬)∘∊˜)/¨(<⊢)) ⊢)


NFmt ← ⟨"¯"⟩‿⟨"-"⟩⊸str.ReplaceAll ∘ •Fmt

FillMissing ← { 
	ts‿p← <˘⍉0‿¯1⊸⊏˘𝕩
	ttake ← (∾⟜1∘÷⟜𝕨∘(1⊸↓-⟜»))ts 
	(∾´(ts) + 𝕨 × ↕¨ ttake) ≍˘ (ttake / p)
} 

# Function to loading quote candlesticks except USDT
LoadQ ← { lquotes ← 𝕩
	qfiles← (⊢∾⟜"_USDT-1h.json")¨ (<"/media/mu6mula/Data/Crypto-Data-Feed/freq-user-data/data/binance/") ∾¨ lquotes
	#•Show lquotes
	#•Show qfiles
	qticks ← (((÷⟜1000∘⊏˘) ∾˘ ((+´÷≠)˘1‿2‿3‿4⊸⊏˘))∘>∘JParse∘•FChars)¨ qfiles
	gbp ← ((FromTS¨⊏˘)∾˘( (+´÷≠)˘1‿2‿3‿4⊸⊏˘))  ((∾⟜0∘(str.ToNums⟨"-"⟩‿⟨" "⟩⊸str.ReplaceAll)¨∘⊏˘)∾˘(•ParseFloat¨˘ (¯1↓˘1⊸↓˘)))>¯1↓76000↓csv.Split•FChars"GBPUSD60.csv"
	3↑¨ qgticks ←  qticks  ∾ <gbp
	lgquotes ← lquotes∾<"GBP"
	lgquotes‿qgticks
}
LoadTrades← {𝕊⋄⍉ ⊢‿⊢‿•ParseFloat‿⊢‿⊢‿•ParseFloat‿•ParseFloat {𝕎 𝕩}¨  ⍉ >csv.SplitL •FLines "trades.csv"}
#3↑ trades← LoadTrades 0
AddQuoteUSDT ← {
	trans ← 𝕩
	# convert DAI USDC BUSD to USDT
	qquotes← ⍷cquote ← ⥊((¬∊⟜"DAI"‿"USDC"‿"BUSD"‿"USD") ⊏˘ ((<"USDT")⊸∾˘)) ⥊˘4⊏˘ trans
	# get unix timstamp and array datetime of transactions, truncated to hours
	10↑ htts← FromTS¨ hdts← (∾⟜0‿0 4⊸↑)¨ dts← (str.ToNums  ⟨"-"⟩‿⟨" "⟩⊸str.ReplaceAll)¨ 1⊏˘ trans
	lquotes ← ((¬∊⟜"GBP"‿"USDT")⊸/) qquotes
	⊑ gquotes‿gticks ← LoadQ lquotes
	usmask←(∊⟜⟨"USDT"⟩) qquotes 
	nuquotes ← (¬usmask)/ qquotes 
	>1↑¨orticks ← (gquotes ⊐ nuquotes) ⊏ 3600⊸FillMissing¨ gticks 
	# group timestamps by quote unit
	qquotes≍≢¨gtrts ← ((⊐cquote)⊔htts) 
	### check for missing quote tickers
	+´¨ (⊏˘¨orticks) (¬⊢∊⊣)¨ (¬usmask) / gtrts
	+´¨ orticks ( ( (⊏˘⊣)⊐⊢) ≥ (≠⊣))¨ (¬usmask)/gtrts

	## Ticker quote price in USDT per transaction (grouped by cquotes)
	2↑¨ gusdp ← (orticks (((⊏˘⊣)⊐⊢)⊏(1⊸⊏˘⊣))¨(¬usmask)/gtrts) ∾ <((≠(⊑/usmask)⊑gtrts)⥊1)
	itrans ← (qquotes ⊐ (∾⟜(<"USDT")) ⊢ (¬usmask) / qquotes) 

	# match quote ticker with transactions, grouped  by quote unit; flatten afterwards
	# append quoteusdt price as last column
	#traqusdt ← ∾´ gusdp (∾˜˘)¨ itrans ⊏ (grtrans ← (⊐cquote)⊔trans)
	traqusdt ← ∾´¨ gusdp ⋈ itrans ⊏ (grtrans ← (⊐cquote)⊔trans)
}
3↑¨ usdtQP‿trades ← AddQuoteUSDT LoadTrades 0

btcf ← ∾⟜("_USDT-1h.json") ("/media/mu6mula/Data/Crypto-Data-Feed/freq-user-data/data/binance/") ∾ "BTC"
2↑ btcTicks ← (((÷⟜1000∘⊏˘) ∾˘ ((+´÷≠)˘1‿2‿3‿4⊸⊏˘))∘>∘JParse∘•FChars) btcf

# transaction timestamps in Unix and array formats
3↑ htts← FromTS¨ hdts← (∾⟜0‿0 4⊸↑)¨ dts← (str.ToNums  ⟨"-"⟩‿⟨" "⟩⊸str.ReplaceAll)¨ 1⊏˘ trades
# check for missing ticker 
+´ (htts) (¬∊) ⊑˘ 3600⊸FillMissing btcTicks
#traqusdt /˜ (htts) (¬∊) ⊑˘ btcTicks

3↑ btcP ← htts ((⊣⊐˜(⊑˘⊢))⊏(1⊸⊑˘⊢)) 3600⊸FillMissing btcTicks

2↑gbpTicks ← ((FromTS¨⊏˘)∾˘( (+´÷≠)˘1‿2‿3‿4⊸⊏˘))  ((∾⟜0∘(str.ToNums⟨"-"⟩‿⟨" "⟩⊸str.ReplaceAll)¨∘⊏˘)∾˘(•ParseFloat¨˘ (¯1↓˘1⊸↓˘)))>¯1↓76000↓csv.Split•FChars"GBPUSD60.csv"
3↑ gbpP ← htts ((⊣⊐˜(⊑˘⊢))⊏(1⊸⊑˘⊢)) 3600⊸FillMissing gbpTicks
	# take base and quote amounts according to side flipping received and spent accordingly
¯3↑¨ bamnt‿qamnt ← <˘⍉ ((1⊸≢¨ 2⊸⊏˘) ⌽˘ (¯2⊸↑˘)) trades
# calculate transaction price in usdt, btc, gbp
3↑¨ abtc‿agbp ← btcP‿gbpP ÷˜¨ <ausdt ← usdtQP × qamnt 
3↑trans ← trades ∾˘ bamnt ∾˘ qamnt ∾˘ ausdt ∾˘ abtc ∾˘ agbp
htrans ← "exchange"‿"time"‿"side"‿"base"‿"quote"‿"received"‿"spent"‿"baseAmt"‿"quoteAmt"‿"usdt"‿"btc"‿"gbp"
"transactions.csv" •FLines csv.JoinL htrans ∾ ({⍉⊢‿⊢‿NFmt‿⊢‿⊢{𝕎𝕩}¨⍉5↑˘𝕩}∾˘{NFmt¨5↓˘𝕩}) trans


## Totals and bags
## Calculate totals by asset
#15↑ inFlows ← (2↑˘ trans) ∾˘ 1 ∾˘ ((1≢¨2⊏˘trans) ⊑¨ <˘⍉3‿4 ⊏ ⍉trans) ∾˘ 5⊏˘trans
#15↑ outFlows ← (2↑˘ trans) ∾˘ ¯1 ∾˘ ((1≡¨2⊏˘trans) ⊑¨ <˘⍉3‿4 ⊏ ⍉trans) ∾˘ -6⊏˘trans
#15↑ inOutFlows ← inFlows ∾ outFlows
#≢inOutFlows
#3↑ assetsH ← ⍷ 3⊏˘ inOutFlows 
#iassets ← ⍋assetsH
#assets ← iassets ⊏ assetsH
#assets (⊑⊒) <"BTC"
#5↑ 12⊑ aflows ←  ((⍋1⊸⊏˘)⊏⊢)¨ iassets ⊏ (⊐(3⊸⊏)˘)⊸⊔ inOutFlows 
#5↑ 10600↓ 12⊑ (∾˘⟜(+`¯1⊸⊏˘))¨ aflows
##>3↑ 13↑¨ sigFlows ← ( (×´¨ <˘))¨ (2‿4⊸⊏˘)¨ aflows 
##1⊸⊏¨aflows
#   13↑ finalAmounts ← (+´ ¯1⊸⊏˘) ¨ aflows 
##>3↑ 9↑¨ cumFlows ←  +`¨ sigFlows 
## 12⊑ aflows ∾˘¨ cumFlows
# assets ∾˘ finalAmounts
# 
#(<¯2⊸↑)˘ trans
#
#
#
# csv.Join 3↑ trans
