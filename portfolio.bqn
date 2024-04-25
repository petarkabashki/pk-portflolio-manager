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
3↑ trans ← ((⍋(ParseTS¨ 1⊸⊏˘))⊸⊏) ∾˘´{⍉⊢‿⊢‿•ParseFloat‿⊢‿⊢{𝕎𝕩}¨⍉𝕩}‿{•ParseFloat¨𝕩} {𝕎 𝕩}¨ 5 (↑˘⋈↓˘) tnd
IfElse ← {cond‿True‿False: cond◶False‿True @}

CalcBalances ← { trans ← 𝕩
	2↑ flows ← ∾´ ⟨3,7,1⟩‿⟨4,8,¯1⟩ {a‿q‿m←𝕨⋄ (3⊸↑˘∾˘a⊸⊏˘∾˘(m⊸× 2⊸⊏˘×q⊸⊏˘)) 𝕩}¨ <trans
	assets ← ⍷3⊏˘flows
	3↑¨ assets { csv.JoinL ⍉⊢‿⊢‿NFmt‿⊢‿NFmt‿NFmt {𝕎 𝕩 }¨ ⍉(∾˘⟜(+`¯1⊸⊏˘)) 𝕩}¨ ((⊐3⊸⊏˘)⊔⊢)  flows
	assets { ("reports/flows/"∾𝕨 ∾ ".csv" ) •FLines (<"xchg,dt,side,asset,qty,cum") ∾ csv.JoinL ⍉⊢‿⊢‿NFmt‿⊢‿NFmt‿NFmt {𝕎 𝕩 }¨ ⍉(∾˘⟜(+`¯1⊸⊏˘)) 𝕩}¨ ((⊐3⊸⊏˘)⊔⊢)  flows

	abals ← assets ≍˘  +´¨ ((⊐3⊸⊏˘)⊔¯1⊸⊏˘)  flows
	"balances.csv" •FLines csv.JoinL ∾˘´ ⊢‿NFmt {𝕎¨𝕩}¨ 1 (↑˘⋈↓˘) (⍋⊏˘)⊸⊏ abals
	abals
}
bals ← CalcBalances trans
### Bags & PNLs
2↑ flows ← ∾´ ⟨3,7,1⟩‿⟨4,8,¯1⟩ {a‿q‿m←𝕨⋄ (2⊸↑˘∾˘ (m×2⊸⊏˘) ∾˘ a⊸⊏˘∾˘(q⊸⊏˘)∾˘9‿10‿11⊸⊏˘) 𝕩}¨ <trans
#15↑ (3⊸⊏˘) flows
#15↑ ((("USDT")⊸≡¨ 3⊸⊏˘)⊸/) flows
assets ← ⍷3⊏˘flows
#⊑assets ⊐< "USDT"
#23⊑assets
5↑ ⊑ grtr ← ((⊐3⊸⊏˘)⊔⊢)  flows
CalcPnl ← {
	>(<0‿0‿0) {
		rb‿rq‿rpnl ← 𝕨   ⋄ sd‿tb‿tq ← 𝕩
		IfElse (sd≡1)‿{𝕤
			(rb+tb)‿(rq+tq)‿0
		}‿{𝕤
			tp←tq÷tb ⋄ 
			#rp ← IfElse (rb≡0)‿{0}‿{(rq÷rb)} ⋄ 
			rp ← {rq÷rb} ⋄ 
			nb←(rb-tb) ⋄ nq ← nb×rp ⋄ pnl ← tb × tp - rp ⋄
			nb‿nq‿pnl
		}
	}` 𝕩  
}
#15↑   2‿4‿5⊸⊏˘ 23⊑grtr
qquotes ← "usdt"‿"btc"‿"gbp"
3↑⊑⊑ pnls ← 5‿6‿7 {
	((⊢∾˘(+`¯1⊸⊏˘)) (CalcPnl(<˘ 2‿4‿𝕨⊸⊏˘)))¨ 𝕩
}¨ <grtr

pnldirs ← {"reports/pnls-"∾𝕩}¨ qquotes 
{{•file.CreateDir 𝕩}⍟(¬•file.Exists) 𝕩}¨ pnldirs 
⊑⊑ pnldirs {d←𝕨⋄  assets { (d∾"/"∾𝕨∾".csv") •FLines (<"xch,dt,base,quote,pnl,cumPnl") ∾ csv.JoinL 𝕩}¨ (2↑˘¨grtr) ∾˘¨ NFmt¨¨ 𝕩 }¨ pnls
2↑⊑bags←((0‿1‿3⊸⊏˘(>¯1⊸⊏¨)))¨pnls
qquotes { ("bags-"∾𝕨∾".csv") •FLines (<"asset,base,quote,pnl") ∾ csv.JoinL ((⍋⊏˘)⊏⊢) assets ∾˘ NFmt¨ 𝕩}¨ bags
# check bags and balances are the same to small rounding error
assets ≡ ⊏˘ bals
1e¯8⊸< +´¨ (<1⊸⊏˘bals) (|-)¨ ⊏˘¨bags
#3↑ ⊑ ParseTS¨¨   (1⊸⊏˘)¨grtr
#•math.isnaN
#13↑¨ (<ParseTS¨¨1⊸⊏˘¨grtr) ((∾˘⟜(+`¯1⊸⊏˘)) (((⍋⊑˘)⊸⊏)(∾´∾˘¨)))¨ 2⊸⊏˘¨¨ pnls


