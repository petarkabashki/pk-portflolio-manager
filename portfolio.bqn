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
((↕≠)≍⊢)∘⊑ tnh‿tnd ← ⊢‿>{𝕎𝕩}¨ 1(⊑∘↑⋈↓) csv.SplitL •FLines "transactions.csv"
3↑ (⍋(ParseTS¨ 1⊸⊏˘)) tnd
3↑ trans ← ((⍋(ParseTS¨ 1⊸⊏˘))⊸⊏) ∾˘´{⍉⊢‿⊢‿•ParseFloat‿⊢‿⊢{𝕎𝕩}¨⍉𝕩}‿{•ParseFloat¨𝕩} {𝕎 𝕩}¨ 5 (↑˘⋈↓˘) tnd

CalcBalances ← { trans ← 𝕩
	2↑ flows ← ∾´ ⟨3,7,1⟩‿⟨4,8,¯1⟩ {a‿q‿m←𝕨⋄ (3⊸↑˘∾˘a⊸⊏˘∾˘(m⊸× 2⊸⊏˘×q⊸⊏˘)) 𝕩}¨ <trans
	assets ← ⍷3⊏˘flows
	#3↑¨ 4 (↑˘⋈↓˘) (∾˘⟜(+`¯1⊸⊏˘)) ⊑ ((⊐2⊸⊏˘)⊔⊢)  flows
	3↑¨ assets { csv.JoinL ⍉⊢‿⊢‿NFmt‿⊢‿NFmt‿NFmt {𝕎 𝕩 }¨ ⍉(∾˘⟜(+`¯1⊸⊏˘)) 𝕩}¨ ((⊐3⊸⊏˘)⊔⊢)  flows
	assets { ("reports/flows/"∾𝕨 ∾ ".csv" ) •FLines (<"xchg,dt,side,asset,qty,cum") ∾ csv.JoinL ⍉⊢‿⊢‿NFmt‿⊢‿NFmt‿NFmt {𝕎 𝕩 }¨ ⍉(∾˘⟜(+`¯1⊸⊏˘)) 𝕩}¨ ((⊐3⊸⊏˘)⊔⊢)  flows

	abals ← (⍋⊏˘)⊸⊏ assets ≍˘ +´¨ ((⊐3⊸⊏˘)⊔¯1⊸⊏˘)  flows
}
"balances.csv" •FLines csv.JoinL ∾˘´ ⊢‿NFmt {𝕎¨𝕩}¨ 1 (↑˘⋈↓˘) CalcBalances trans



