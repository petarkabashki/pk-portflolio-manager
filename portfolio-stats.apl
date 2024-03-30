⍝ #!/usr/local/bin/dyalogscript

⍝ --------------------------------------------------------------------
⍝ ----------------------  PORTFOLIO STATS  ----------------------------
⍝ --------------------------------------------------------------------

⍝ --- Load latest bags from file
hbags ← 'asset' 'size' 'avgPrice' 'total' 'cupnl'
(lbags hbags)  ← ⎕CSV 'latest-bags.csv' '' (1 2 2 2 2) 1 
]display hbags⍪lbags

⍝ --- Load latest prices/quotes
(dq hq)←⎕CSV'cmc-quotes.csv' '' ⍬ 1 ⍝ load trades
]display (⊂2 3)⌷[2] ⊢ dq
⍝ ]display  dq←{⍵[⍋⍵[;2];]}⊢ dq
⍝ ]display dq← {(~⍵[;2]∊'GBP' 'USDT' 'USDC' 'BUSD')⌿⍵ }dq

⍝ --- check lists are equivalent 
⍝ dq[;2] ≢¨ {⍵[⍋⍵]}lbags[;1]

⍝ --- Current prices 
⍝ toN← (⍎('-'⎕R'¯')∘('e'⎕R'E')) ⍝ function to convert string to number 
⍝ cp← ({⍵[;,1]},(toN¨∘(2⌷[2]⊢)))  ((⊂2 3)⌷[2])  dq
]display cp← {⍵[;,1], (⍎¨∘('-'⎕R'¯')∘('e'⎕R'E')) ⍵[;2]}({ (~⍵[;1]∊('GBP' 'USDT' 'BUSD' 'USDC' 'DAI'))⌿⍵ }) dq[;2 3]

⍝ --- Missing prices
lbags[;1] ~ cp[;1]
⍝ --- latest prices in the order of lbags; USD equivalents are mapped to 1
]display lbags[;,1], lbp←(⊂cp[;1] ⍳ lbags[;1])⌷((2⌷[2]⊢) cp⍪('MISSING' 1)) 
⍝ --- price change relative to average acquisition price
⎕←pch ← 1-⍨lbp÷lbags[;3]
⍝ --- Current portfolio state
⍝ pfstate← cp {pc←¯1+⍺÷ ⍵[;,3]⋄⍵,⍺,pc,(pc×⍵[;,4]),⍺×⍵[;,2] } lbags
pfstate←(lbp×lbags[;2]),⍨(pch×lbags[;4]),⍨pch,⍨lbags,lbp
pfstate (⎕CSV⍠'IfExists' 'Replace') 'portfolio.csv' 
⍝ {⍵[;6]} ⊢{(⍵[;1]≢¨⊂'GBP')⌿⍵} ⊢ dq {⍵,⍺[;3]} {⍵[⍋⍵[;1];]} lbags
(({(~⍵[;1]∊('GBP' 'USDT' 'BUSD' 'USDC' 'DAI'))⌿⍵}) pfstate) (⎕CSV⍠'IfExists' 'Replace') 'portfolio.csv' 
(({(~⍵[;1]∊('GBP' 'USDT' 'BUSD' 'USDC' 'DAI' 'BTC' 'GRT' 'ADA' 'ALGO' 'ETH' 'ATOM' 'BNB' 'CAKE' 'LUNA' 'THETA' 'AAVE' 'LRC' 'FTM' 'TRX' 'ZIL' 'LTC' 'KSM' 'XRP' 'XLM' 'ENJ' 'DOGE' 'LINK' 'CRV' 'ICP'))⌿⍵}) pfstate) (⎕CSV⍠'IfExists' 'Replace') 'portfolio-2024.csv' 
⍝ --- Export transactions and rolling bags
⍝ ( (⊃dtr[1]),(⊃rbags[1]) ) (htr, hbags) (⎕CSV⍠'IfExists' 'Replace') 'tran-bags.csv' 
