

⟨CsvSplit⇐Split,Join,CsvSplitL⇐SplitL,JoinL⟩←•Import "bqn-libs/csv.bqn"
⟨StrSplit⇐Split⟩←•Import "bqn-libs/strings.bqn"
ToUpper ← -⟜(32×1="a{"⊸⍋)
RowNu ← { ((↕≠𝕩)-𝕨) ∾˘ 𝕩}
##########################################
# Phemex

PhemTra← { 𝕤
	files←(<"../trading-data/phemex/") ∾¨ "SPOT_TRADE_2023"‿"SPOT_TRADE_2024-interim" ∾¨ (<".csv")
	data← >∾´(1⊸↓ >CsvSplitL  •FLines)¨  files

	side← ToUpper¨ 2⊏˘data
	iside← ¯1+ 2×side ≡¨ (<"BUY")
	base‿quote ← <˘⍉> " / "⊸StrSplit¨ 1⊏˘data
	xprice←•ParseFloat¨ 4⊏˘data
	amrecQ‿amrecA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 3⊏˘data
	amspeQ‿amspeA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 5⊏˘data
	amQ‿amA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 6⊏˘data
	amrecQ
	amspeQ
	7 ↑ ⍉amrecQ ≍ amspeQ
	 idxbuy ← (iside ≡¨ 1)
	 qty← (1-idxbuy) ⊑¨ <˘⍉amrecQ ≍ amspeQ
	 total← (idxbuy) ⊑¨ <˘⍉amrecQ ≍ amspeQ
#10↑ (<˘⍉>(⊢⋈(1⊸-)) (iside ≢¨ ¯1)) ⊏¨ <⍉amrecQ ≍ amspeQ
	#feeQ‿feeA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 9⊏˘data
	# checks
	#+´(xprice ⋆ iside) - amspeQ ÷ amrecQ
	#+´ (iside = 1) ∧ (amrecA ≢¨ base) ∨ (amspeA ≢¨ quote)
	#+´ (iside = ¯1) ∧ (amspeA ≢¨ base) ∨ (amrecA ≢¨ quote)
	("ts"‿"iside"‿"base"‿"quote"‿"qty"‿"total") ∾ ⍉[(⊏˘ data), iside, base, quote, qty, total]
}

#PhemTra 0 


##########################################
# Binance

#BinaTra← { 𝕤
	files←(<"../trading-data/binance/OrderHistory/") ∾¨ "2024-02-22 19_27_28"‿"2023"‿"2022"‿"2021"‿"2020" ∾¨ (<".csv")
	#data← ⟨ 1, 10, 697, 698, 1398, 1605, 1606, 1752, 1855, 1859, 1869, 1870 ⟩ ⊏ >∾´(1⊸↓ >CsvSplitL  •FLines)¨  files
	data← >∾´(1⊸↓ >CsvSplitL  •FLines)¨  files

	side← ToUpper¨ 4⊏˘data
	iside← ¯1+ 2×side ≡¨ (<"BUY")
	qassets←"BTC"‿"USDT"‿"BUSD"‿"GBP"‿"USDC"‿"UST"‿"DAI"‿"ETH"
	qassets ∾ ⍉ qassets (+´∘/∘⍷)⌜ 10↑ 2 ⊏˘ data
#10↑ data
	 pairs ← 2 ⊏˘ data
	 tqpos← ⍉ qassets (+´∘/∘(¯1⊸⊑ × ⊢)∘⍷)⌜ pairs
	 qpos ← ⌈˝˘ tqpos 
	≢data 
(/qpos ≡¨0)
	 (/qpos ≡¨0) ⊏ data
	 quote← ⥊((/⟜qassets)˘(0⊸≢)¨) tqpos 
	 base ← qpos	↑¨ pairs
	  execcol ← 8⊏˘data
	  trtotcol ← 10⊏˘data
	 avgprice ← •ParseFloat¨ 9⊏˘data
	 # Check base is always in the "Executed" column
	 +´(1⊸≢∘¯1⊸⊑)¨ base ⍷¨ execcol
	 # Check quote is always in the "Trading total" column
	 +´(1⊸≢∘¯1⊸⊑)¨ quote ⍷¨ trtotcol
	 total← •ParseFloat¨ (-≠¨quote) ↓¨ trtotcol

	 
	 executed ← •ParseFloat¨ (-qpos) ↓¨ execcol
		+´avgprice - total ÷ executed + 1e¯18
		+´0≢¨executed
	("ts"‿"iside"‿"base"‿"quote"‿"qty"‿"total") ∾ ⍉[(7⊏˘ data), iside, base, quote, executed, total]
1 RowNu ⍉ qassets ∾˘ ((0⊸≢))¨ qassets (+´∘/∘(¯1⊸⊑ × ⊢)∘⍷)⌜ 10↑ 2 ⊏˘ data
 ⍉ <˘⍉((0⊸≢))¨ qassets (+´∘/∘(¯1⊸⊑ × ⊢)∘⍷)⌜ 10↑ 2 ⊏˘ data
(¯1⊸⊑ × ⊢) ⟨0,0,0,1,0⟩
0 RowNu ⍉ 	qassets (⍷)⌜ 10↑ 2 ⊏˘ data
	5 ⊏data
	base‿quote ← <˘⍉> " / "⊸StrSplit¨ 1⊏˘data
	xprice←•ParseFloat¨ 4⊏˘data
	amrecQ‿amrecA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 3⊏˘data
	amspeQ‿amspeA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 5⊏˘data
	amQ‿amA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 6⊏˘data
	#feeQ‿feeA ←((•ParseFloat¨∘⊏)⋈(1⊸⊏)) ⍉>" "⊸StrSplit¨ 9⊏˘data
	# checks
	#+´(xprice ⋆ iside) - amspeQ ÷ amrecQ
	#+´ (iside = 1) ∧ (amrecA ≢¨ base) ∨ (amspeA ≢¨ quote)
	#+´ (iside = ¯1) ∧ (amspeA ≢¨ base) ∨ (amrecA ≢¨ quote)
	("ts"‿"iside"‿"base"‿"quote"‿"amrec"‿"amspe") ∾ ⍉[(⊏˘ data), iside, base, quote, amrecQ, amspeQ]
}

#PhemTra 0 

b

fd←  ⍉≍⌽ ≠¨ Split •FChars ⊑ ph_files
fd←  ⍉≍ (1⊸↓-¯1⊸↓) ≠¨ Split •FChars ⊑ ph_files

←>JParse •FChars dfld∾"ALGO_USDT-8h.json" 
3 + 3

