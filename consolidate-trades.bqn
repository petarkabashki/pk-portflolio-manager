
⟨FromTS⇐FromTimestamp, ToTS⇐ToTimestamp⟩ ← •Import "bqn-libs/datetime.bqn"
⟨JParse⇐Parse⟩ ← •Import "bqn-libs/json.bqn"
csv←•Import "bqn-libs/csv.bqn"
str←•Import "bqn-libs/strings.bqn"
ToUpper ← -⟜(32×1="a{"⊸⍋)
RowNu ← { ((↕≠𝕩)-𝕨) ∾˘ 𝕩}
SplitNum ←•Parsefloat‿⊢{𝕎 𝕩}¨(("-."∾'0' + ↕10) (((⊢⋈¬)∘∊˜)/¨(<⊢)) ⊢)
##########################################
# Phemex
# TODO: Incorporate fees in received/spent if appropriate

PhemexTra← { 𝕤
	files←(<"../trading-data/phemex/") ∾¨ "SPOT_TRADE_2023"‿"SPOT_TRADE_2024-interim" ∾¨ (<".csv")
	data← >∾´(1⊸↓ >csv.SplitL  •FLines)¨  files
	tscol ← ⊏˘ data
	side← ToUpper¨ 2⊏˘data
	iside← ¯1+ 2×side ≡¨ (<"BUY")
	base‿quote ← <˘⍉> " / "⊸str.Split¨ 1⊏˘data
	xprice←•ParseFloat¨ 4⊏˘data
	received‿receivedU ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸str.Split¨ 3⊏˘data
	spent‿spentU ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸str.Split¨ 5⊏˘data
	#fee‿feeU ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸str.Split¨ 9⊏˘data
	("exchange"‿"ts"‿"iside"‿"base"‿"quote"‿"received"‿"spent") ∾ (<"phemex") ∾˘ ⍉[tscol, iside, base, quote, received, spent]
}

#5↑PhemexTra 0 


##########################################
# Binance
# TODO: Download 2024 spot order history and include in the calculations`

# Replace with SpotOrder logs and include transactions
#BinaTraOrderHist← { 𝕤
#	files←(<"../trading-data/binance/OrderHistory/") ∾¨ "2024-02-22 19_27_28"‿"2023"‿"2022"‿"2021"‿"2020" ∾¨ (<".csv")
#	#data← ⟨ 1, 10, 697, 698, 1398, 1605, 1606, 1752, 1855, 1859, 1869, 1870 ⟩ ⊏ >∾´(1⊸↓ >csv.SplitL  •FLines)¨  files
#	data← >∾´(1⊸↓ >csv.SplitL  •FLines)¨  files
#
#	side← ToUpper¨ 4⊏˘data
#	iside← ¯1+ 2×side ≡¨ (<"BUY")
#	qassets←"BTC"‿"USDT"‿"BUSD"‿"GBP"‿"USDC"‿"UST"‿"DAI"‿"ETH"
#	qassets ∾ ⍉ qassets (+´∘/∘⍷)⌜ 10↑ 2 ⊏˘ data
##10↑ data
#	 pairs ← 2 ⊏˘ data
#	 tqpos← ⍉ qassets (+´∘/∘(¯1⊸⊑ × ⊢)∘⍷)⌜ pairs
#	 qpos ← ⌈˝˘ tqpos 
##	≢data 
##(/qpos ≡¨0)
#	 (/qpos ≡¨0) ⊏ data
#	 quote← ⥊((/⟜qassets)˘(0⊸≢)¨) tqpos 
#	 base ← qpos	↑¨ pairs
#	  execcol ← 8⊏˘data
#	  trtotcol ← 10⊏˘data
#	 avgprice ← •ParseFloat¨ 9⊏˘data
#	 # Check base is always in the "Executed" column
#	 +´(1⊸≢∘¯1⊸⊑)¨ base ⍷¨ execcol
#	 # Check quote is always in the "Trading total" column
#	 +´(1⊸≢∘¯1⊸⊑)¨ quote ⍷¨ trtotcol
#	 total← •ParseFloat¨ (-≠¨quote) ↓¨ trtotcol
#
#	 
#	 executed ← •ParseFloat¨ (-qpos) ↓¨ execcol
#		+´avgprice - total ÷ executed + 1e¯18
#		+´0≢¨executed
#	("ts"‿"iside"‿"base"‿"quote"‿"qty"‿"total") ∾ (executed ≢¨1) / ⍉[(7⊏˘ data), iside, base, quote, executed, total]
#}


BinanceTra← { 𝕤
	files←(<"../trading-data/binance/SpotOrder/") ∾¨ "2023"‿"2022"‿"2021"‿"2020" ∾¨ (<".csv")
	data← >∾´(1⊸↓ >csv.SplitL  •FLines)¨  files
	side← ToUpper¨ 2⊏˘data
	iside← ¯1+ 2×side ≡¨ (<"BUY")
	qassets←"BTC"‿"USDT"‿"BUSD"‿"GBP"‿"USDC"‿"UST"‿"DAI"‿"ETH"
	pairs ← 1 ⊏˘ data
	tqpos← ⍉ qassets (+´∘/∘(¯1⊸⊑ × ⊢)∘⍷)⌜ pairs
	qpos ← ⌈˝˘ tqpos 
	quote← ⥊((/⟜qassets)˘(0⊸≢)¨) tqpos 
	base ← qpos	↑¨ pairs
	tscol ← ⊏˘data
	exec‿execU ← <˘⍉>SplitNum¨ 4⊏˘data
	amount‿amountU ← <˘⍉>SplitNum¨ 5⊏˘data
	fee‿feeU ← <˘⍉>SplitNum¨ 6⊏˘data
	# Check base unit == executed unit
	+´base ≢¨ execU
	# Check quote is always in the "Trading total" column
	+´quote ≢¨ amountU
	#iside ∾ exec ≍ amount
	received‿spent ← <˘⍉>(iside≢¨1) ⌽¨ <˘⍉ exec ≍ amount
	### Include fee in qty / total for buy / sell
	#total ← amount - fee × iside ≡¨ ¯1 
	#qty ← executed - fee × iside ≡¨ 1 
	########################
	### Fee is not included in the total, needs to be subtracted from the received amount or applied otherwise
	("exchange"‿"ts"‿"iside"‿"base"‿"quote"‿"received"‿"spent") ∾ (<"binance") ∾˘ ⍉[tscol, iside, base, quote, received, spent]
}
#5↑BinanceTra 0

#############################################
#  Coinbase
CoinbaseProTra← { 𝕤
	files←(<"../trading-data/coinbase-pro/") ∾¨ ⥊ (<"fills-2023-12-22") ∾¨ (<".csv")
	data← ((⍋4⊸⊏˘)⊏⊢) >∾´(1⊸↓ >csv.SplitL  •FLines)¨  files
	tscol ← 4⊏˘ data
	side← ToUpper¨ 3⊏˘data
	iside← ¯1+ 2×side ≡¨ (<"BUY")
	base‿quote ← <˘⍉> "-"⊸str.Split¨ 2⊏˘data
	# check side unit is either base or quote
	+´1≢¨(⊑∘⥊)¨ (<¨6⊏˘data) ∊¨ (<˘⍉base≍quote)
	# check fee/total unit is either base or quote
	+´1≢¨(⊑∘⥊)¨ (<¨10⊏˘data) ∊¨ (<˘⍉base≍quote)
	size ← •ParseFloat¨ 5⊏˘data
	sizeU ← 6⊏˘data
	total ← •ParseFloat¨ 9⊏˘data
	totalU ← 10⊏˘data
	price←•ParseFloat¨ 7⊏˘data
	calcTotal ← price ×¨ size
	indReceived ← (base ≡¨ totalU) ≡¨ (iside≡¨1) 
	indSpent ← 1 - indReceived
	size_tot ← <˘⍉ size ≍ calcTotal 	
	received ← indReceived ⊑¨ size_tot 
	spent ← indSpent ⊑¨ size_tot
	#fee ← •ParseFloat¨ 8⊏˘data
	#feeU ← totalU
	# check feeU == quote
	+´ totalU ≢¨ quote
	("exchange"‿"ts"‿"iside"‿"base"‿"quote"‿"received"‿"spent") ∾ (<"coinbase-pro") ∾˘ ⍉[tscol, iside, base, quote, received, spent]
}
30↓CoinbaseProTra 0 

#############################################
#  Kucoin
KucoinTra← { 𝕤
	files←(<"../trading-data/kucoin/Completed_Trades-") ∾¨ ("2021"‿"2022") ∾¨ (<".csv")
	data← >∾´(1⊸↓ >csv.SplitL  •FLines)¨  files
	tscol ← 10⊏˘ data
	side← ToUpper¨ 4⊏˘data
	iside← ¯1+ 2×side ≡¨ (<"BUY")
	base‿quote ← <˘⍉> "-"⊸str.Split¨ 3⊏˘data
	#fee ← •ParseFloat¨ 11⊏˘data
	#feeU← ToUpper¨ 13⊏˘data
	# check fee unit is always the quote
	#+´feeU ≢¨ quote

	price←•parsefloat¨ 6⊏˘data
	amount←•parsefloat¨ 7⊏˘data
	volume←•parsefloat¨ 8⊏˘data
	# check volume = price × amount
	1e¯6 < +´ | volume - price × amount
  #iside ∾ amount ≍ volume
	received‿spent ← <˘⍉>(iside ≢¨ 1) ⌽¨ amount (<∘≍)˘ volume
	35↓("exchange"‿"ts"‿"iside"‿"base"‿"quote"‿"received"‿"spent") ∾ (<"kucoin") ∾˘ ⍉[tscol, iside, base, quote, received, spent]
}
#5↑ KucoinTra 0 

#############################################
#  BitGet
BitgetTra← { 𝕤
	files←(<"../trading-data/bitget/Spot Order History-") ∾¨ ("2023"‿"2024-prelim") ∾¨ (<".csv")
	# filter out cancelled
	data← (((<"cancelled")≢¨8⊸⊏˘) / ⊢) >∾´(1⊸↓ >csv.SplitL  •FLines)¨  files
	tscol ← 0⊏˘ data
	side← ToUpper¨ 3⊏˘data
	iside← ¯1+ 2×side ≡¨ (<"BUY")
	qassets←"BTC"‿"USDT"‿"BUSD"‿"GBP"‿"USDC"‿"UST"‿"DAI"‿"ETH"
	pairs ← ¯5↓¨2 ⊏˘ data
	tqpos← ⍉ qassets (+´∘/∘(¯1⊸⊑ × ⊢)∘⍷)⌜ pairs
	qpos ← ⌈˝˘ tqpos 
	quote← ⥊((/⟜qassets)˘(0⊸≢)¨) tqpos 
	base ← qpos	↑¨ pairs
	avgPrice←•parsefloat¨ 7⊏˘data
	executed←•parsefloat¨ 6⊏˘data
	calcTotal ← avgPrice × executed
	iside∾⍉> executed (<∘≍)˘ calcTotal
	received‿spent ← <˘⍉>(iside ≢¨ 1) ⌽¨ executed (<∘≍)˘ calcTotal
	# apply 0.1% manually
	#fee ← 0.001 × received
	#feeU ← (iside ≢¨ 1) ⊑¨ base (<∘≍)˘ quote
	("exchange"‿"ts"‿"iside"‿"base"‿"quote"‿"received"‿"spent") ∾ (<"bitget") ∾˘ ⍉[tscol, iside, base, quote, received, spent]
}
#5↑ BitgetTra 0 
# TODO: add Kraken
#############################################
#  Kraken
#############################################

#############################################
### Consolidate all trades

# Load consolidated transactions ordered by timestamp
≢ trans←  ((⍋1⊸⊏˘)⊏⊢) (⊏˘ ∾˘ ((10⊸↑⊸∾⟜" "⊸∾⟜(8⊸↑ 11⊸↓))¨ 1⊸⊏˘) ∾˘ 2⊸↓˘) ∾´ BinanceTra‿PhemexTra‿KucoinTra‿BitgetTra‿CoinbaseProTra { 1↓ 𝕎 𝕩}¨ <0

#3↑ trans
#csv.Join 2‿3⥊↑"BQNcs"
#3↑  csv.JoinL ⊢ ⍉ ⊢‿⊢‿•Fmt‿⊢‿⊢‿•Fmt‿•Fmt {𝕎 𝕩}¨ ⍉ trans
NumFmt ← ⟨"¯"⟩‿⟨"-"⟩⊸str.ReplaceAll ∘ •Fmt
#NumFmt ¯100
#≢ csv.JoinL ⊢ ⍉ ⊢‿⊢‿NumFmt‿⊢‿⊢‿NumFmt‿NumFmt {𝕎 𝕩}¨ ⍉ trans
"trades.csv" •FLines csv.JoinL ⊢ ⍉ ⊢‿⊢‿NumFmt‿⊢‿⊢‿NumFmt‿NumFmt {𝕎 𝕩}¨ ⍉ trans

