⟨FromTS⇐FromTimestamp, ToTS⇐ToTimestamp⟩ ← •Import "bqn-libs/datetime.bqn"
⟨JParse⇐Parse⟩ ← •Import "bqn-libs/json.bqn"
csv←•Import "bqn-libs/csv.bqn"
str←•Import "bqn-libs/strings.bqn"
ToUpper ← -⟜(32×1="a{"⊸⍋)
RowNu ← { ((↕≠𝕩)-𝕨) ∾˘ 𝕩}
SplitNum ←•Parsefloat‿⊢{𝕎 𝕩}¨(("-."∾'0' + ↕10) (((⊢⋈¬)∘∊˜)/¨(<⊢)) ⊢)
NFmt ← ⟨"¯"⟩‿⟨"-"⟩⊸str.ReplaceAll ∘ •Fmt
ParseDT ← (∾⟜0‿0 4⊸↑) (str.ToNums  ⟨"-"⟩‿⟨" "⟩⊸str.ReplaceAll) 
ParseTS ← FromTS ParseDT
IfElse ← {cond‿True‿False: cond◶False‿True @}


((↕≠)≍⊢)∘⊑ bgh‿bgd ← ⊢‿>{𝕎𝕩}¨ 1(⊑∘↑⋈↓) csv.SplitL •FLines "bags.csv"

⍉ 3↑ bgd
skip ← "USDT"‿"GBP"‿"BUSD"‿"DAI"‿"USD"
3↑ bags← (⍋⊏˘)⊸⊏ (¬∘⊏˘∊⟜skip)⊸/ (⊏˘∾˘(•ParseFloat¨ 1⊸↓˘)) ⟨"""","NaN"⟩‿⟨"","0"⟩⊸str.ReplaceAll¨ bgd 
bagAssets←⊏˘bags

3↑⍉>cmcAssets‿cmcPriceUSDT ← <˘⊢‿•ParseFloat {𝕎𝕩}¨⍉(⍋⊏˘)⊸⊏(¬∘⊏˘∊⟜skip)⊸/>1‿2⊸⊏¨ 1↓ csv.SplitL •Flines "cmc-quotes.csv"
# check all asset prices are downloaded
(¬bagAssets ∊ cmcAssets) / bagAssets
+´(¬bagAssets ∊ cmcAssets)

btcCmc ← (⊑cmcAssets ⊐ <"BTC") ⊑ cmcPriceUSDT
cmcPriceBTC ← cmcPriceUSDT ÷ btcCmc
ocmcPriceUSDT‿ocmcPriceBTC ← (cmcAssets ⊐ bagAssets)⊸⊏¨  cmcPriceUSDT‿cmcPriceBTC
bgQty‿bgValUSDT‿bgValBTC ←  1‿2‿5 ⊏˘¨ <bags
bgPriceUSDT‿bgPriceBTC ←  bgValUSDT‿bgValBTC ÷¨ <bgQty
priceChUSDT‿priceChBTC ← ocmcPriceUSDT‿ocmcPriceBTC - bgPriceUSDT‿bgPriceBTC
pctChUSDT‿pctChBTC ← priceChUSDT‿priceChBTC ÷ bgPriceUSDT‿bgPriceBTC
vChUSDT‿vChBTC ← priceChUSDT‿priceChBTC ×¨ <bgQty
currValUSDT‿currValBTC ← ocmcPriceUSDT‿ocmcPriceBTC ×¨ <bgQty
3↑tblUSDT ← bgQTY∾˘bgPriceUSDT∾˘bgValUSDT∾˘ocmcPriceUSDT∾˘pctChUSDT∾˘vChUSDT∾˘currValUSDT
3↑tblBTC←bgQty∾˘bgPriceBTC∾˘bgValBTC∾˘ocmcPriceBTC∾˘pctChBTC∾˘vChBTC∾˘currValBTC
poSkip ← "BNB"‿"USDC"
balThre ← 100<bgValUSDT
"portfolio-btc.csv" •FLines (<"asset,bgQty,bgPrice,bgVal,ocmcPrice,pctChange,vChange,currVal") ∾ csv.JoinL (balThre × ¬bagAssets ∊ poSkip)⊸/ bagAssets∾˘ NFmt¨ tblBTC
"portfolio-usdt.csv" •FLines (<"asset,bgQty,bgPrice,bgVal,ocmcPrice,pctChange,vChange,currVal") ∾ csv.JoinL (balThre × ¬bagAssets ∊ poSkip)⊸/ bagAssets∾˘ NFmt¨ tblUSDT

