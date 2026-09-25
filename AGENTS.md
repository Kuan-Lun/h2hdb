# AGENTS.md

## 政策來源

- 本檔是此 repository 的唯一代理開發政策來源。
- 其他代理入口只能要求完整閱讀本檔，不得複製另一份政策。
- 可執行規則以 repository 內的 scripts 與設定檔為準。

## 溝通

- 最終回覆一律使用繁體中文。
- 程式碼、識別字、命令、檔名與 commit message 可使用英文。
- 不得為了承載回覆而新增 Markdown 文件。
- 移除 compatibility path、改變公開行為或採用例外時，必須在對話及
  最終回覆中明確說明。

## 設計與修改原則

- 除使用者在目前任務明確指定限制外，設計時不以最小修改或向後相容
  作為限制；在已授權任務範圍內以程式碼品質優先，選擇架構、可讀性與
  可測試性最好的完整結果。
- 開始實作時須說明上述設計原則；完成時須如實說明是否遵守，以及具體
  的設計取捨。若實際受到最小修改或向後相容限制，須揭露限制來源及
  影響，不得宣稱未受限制。
- 綜合考慮 SOLID、KISS、YAGNI、內聚性與低耦合。
- 必要的局部重構可直接納入任務。
- 若會實質擴大任務範圍、改變原要求未涵蓋的公開行為，或引入資料遷移，
  必須先取得使用者同意。
- 任務直接涉及的 legacy compatibility code 應移除，不保留 shim；不全面
  清理與任務無關的 legacy code。
- generated output 不得直接修改；必須修改 generator 或 source 後重新產生。

## 工作樹與 Git

- 唯讀分析不建立 branch。
- 凡會修改 tracked files 的任務，使用
  `scripts/detect-primary-branch.sh` 判定 primary，並建立專用 task branch。
- 不得 stash、reset、clean、覆寫或混入既有使用者修改。
- 工作樹不乾淨時，從 committed primary 建立獨立 worktree。
- task branch 可包含多個邏輯 Conventional Commits。避免巨大 commit；小而
  內聚的任務仍可只有一個 commit。
- 任務完成後執行 `scripts/git-flow-merge.sh`。該腳本負責 exact-tree release
  gate、`--no-ff --no-commit` candidate、自動 Codex code review、最後的 merge
  commit、安全移除 task worktree，以及以 `git branch -d` 刪除已合併的本機
  branch。Review 失敗亦須 abort merge並保留 task branch。
- task branch 與 primary 只須有 common ancestor；不得要求 task branch 必須
  包含 primary 的最新 tip。分歧由正常 three-way merge處理。
- merge conflict 或 gate failure 時必須 abort merge並保留 task branch。
- merge 後收到的任何 follow-up 都建立新的 task branch。
- Primary 不得 rebase，以免移除或改寫已完成的 merge commit。安裝 hooks 時
  設定 `pull.rebase=false`、`branch.<primary>.rebase=false` 與 `pull.ff=only`；
  pull 遇到分歧必須停止，再透過明確的 merge 流程處理。Task branch 整合仍
  使用 `--no-ff`，`pre-rebase` 必須拒絕對 primary 的顯式 rebase。
- 本機 task branch、commit、`--no-ff` merge與 `branch -d` 已獲預先授權。
- fetch、pull、push、remote branch、tag、release、publish、deploy與任何
  force操作仍須逐次明確授權。
- 不得使用 `--no-verify`。

## 提交格式

- 所有非 merge commit 必須符合 Conventional Commits。
- Breaking change 使用 `type!:` 或 `BREAKING CHANGE:` footer。
- project version 更新使用獨立 commit：
  `chore(release): bump version to X.Y.Z`。

## 版本政策

- `pyproject.toml` 的 `[project].version` 是唯一 project version source。
- project version 固定使用 `X.Y.Z`。
- 歷史 legacy `0.23.0.11` 只容許在本次 policy bootstrap 正規化為
  `0.23.1`；後續不得再使用四段版號。
- 1.0 前，`Y` 是 compatibility lane，`Z` 是同一 lane 內的相容 release
  counter。相容修正或功能遞增 `Z`；breaking change遞增 `Y` 並將 `Z`
  歸零。
- 1.0 後使用標準 Semantic Versioning。
- 整個 task branch只在整合前更新一次 project version。
- shipped runtime、schema或 deployment surface 有變更時，至少需要相容
  升版。
- Breaking API、CLI、config、schema、protocol、資料格式或 Python/platform
  support變更必須提高 compatibility lane或 major。
- tests、一般文件、IDE、hooks、CI與 dev-only tooling 單獨變更時不升版。
- 未分類路徑必須明確判定 impact，不得靜默當作 `none`。
- 已證實不改 artifact或行為的格式化、註解或重構可在 task commits加入
  `Version-Impact: none` 與非空白的 `Version-Reason:`；最終回覆也必須揭露。
- `scripts/check-version.py` 以整個 task相對 primary的差異判定一次，不逐
  commit升版；merge candidate使用 staged index tree判定。
- project version變更必須包含由 `scripts/audit-dependencies.py` 產生且已
  人工審閱的 `.release/dependency-audit.json`。

## 依賴與環境

- repository 必須能從單一乾淨 checkout重建，不得依賴固定 sibling clone
  路徑。
- 明確跨 repository任務可使用傳入的 wheel、Git URL/ref或 repository
  path；sibling discovery只能是選擇性的效能優化。
- Python registry dependencies原則上使用 `>=` lower bound；合理 upper
  bound與 `!=` 可以保留，但必須有相容性依據。
- 精確版本只允許經驗證且有文件理由的特殊契約。
- dependency audit必須涵蓋 build、runtime、optional、development與 Node
  direct dependencies，並記錄現有 upper bound之外的 registry最新版。
- audit script負責盤點與候選發現；有新版時仍須檢查 release notes、驗證
  相容性並嘗試修正問題，再以具體 review note產生 receipt。
- audit receipt綁定 project version與 dependency manifest hash；它本身不
  取代 bounded merge profile，manual deep結果亦必須另行回報。
  exact-tree release receipt會連同 audit evidence一起綁定 merge candidate。
- `uv.lock` 與 `package-lock.json` 不得成為 committed或重建輸入。
  `scripts/rebuild-env.sh` 可使用 `uv venv` 與 `uv pip`，但不得使用會依賴
  project lockfile的同步流程。
- Node tooling使用 `npm install --package-lock=false`。
- 不得依賴 system-wide lint、format、type-check或 Markdown工具。
- `requires-python` 使用 `>=3.14`；只有經驗證的壞版本可使用 `!=`。

## 品質工具

- `pyproject.toml` 是 Ruff與 mypy的唯一規則來源。
- 使用 Ruff lint與 Ruff formatter，不使用 Black。
- Ruff使用適合本專案的嚴格規則集，不從 `ALL` 出發；每個停用規則必須
  在設定旁記錄理由。
- mypy使用標準 `strict = true`。不得保留 `mypy.ini`。
- module例外使用精確 TOML overrides。
- `type: ignore` 必須指定 error code並附理由。
- `noqa` 必須指定 rule code並附理由。
- Markdown使用 repository-local `markdownlint-cli2`。
- VS Code使用同一份 `pyproject.toml`、Markdown設定與 `.venv`；CLI gate是
  最終權威，IDE diagnostics為即時輔助。

## 檢查與 release receipt

- `scripts/format.sh`：明確執行會修改檔案的 Ruff fixer、Ruff formatter與
  Markdown fixer。
- `scripts/check-fast.sh`：離線、唯讀的 Ruff、format check、strict mypy與
  markdownlint；每次非 merge commit執行。不得寫入 Ruff或 mypy cache。
- `scripts/run-pytest.py merge`是 canonical bounded pytest runner。它先執行
  `not deep and not mariadb`，再以單一 worker、`H2HDB_TEST_MARIADB=1`執行
  `mariadb_smoke and mariadb and not deep`；collection、execution、teardown、
  pytest/xdist owned process-tree cleanup與兩階段間 overhead共用 300 秒 hard
  deadline。POSIX使用新session/process group；Windows必須先把 start-gated
  supervisor指派到 kill-on-close Job Object，才可啟動pytest，`taskkill /T`只可
  作為Job termination失敗的 bounded fallback。每一phase使用新的owner，且前一
  phase未取得empty-tree receipt時不得啟動下一phase。刻意脫離POSIX process
  group/session的test child不在該平台保證內。Docker daemon內的
  Testcontainers/Ryuk resource cleanup可能在runner結束後才完成，不屬於此
  deadline或owned process-tree的證據。
- `scripts/check-full.sh`：fast gate、coverage contract、schema與 generated
  artifact drift、schema surface、Lean、上述 bounded pytest merge profile、
  small TLC profiles，以及 distribution boundary。
- `scripts/check-pytest-deep.sh`是明確的手動 deep pytest入口；它預設不限時，
  依序執行完整 non-MariaDB與完整 live MariaDB suite。deep pytest與 deep TLC
  都不進入自動 merge gate。
- 清理、compaction或其交易快取的修改，另執行手動
  `scripts/run-pytest.py cleanup-acceptance`，涵蓋兩個 backend的小型真實
  compaction、正式深度邊界與清理／稽核恢復契約；它不是完整 deep suite。
  跨 ingest／OPDS的發布與清理修改，另以實際部署 Compose衍生的隔離環境執行
  `scripts/check-deployment-acceptance.py --instrumented --cleanup-faults`；
  發布、後續清理 DONE與下一次工作取得須分開驗證。只完成 publication的
  baseline不得宣稱通過完整清理驗收。這些手動結果須另行回報，不屬於
  bounded merge receipt。
- 部署驗收工具的變更另以明確含 ingest與 Pillow的
  `H2HDB_ACCEPTANCE_PYTHON`執行 `tests/test_deployment_acceptance_fixture.py`
  的 `test_real`案例；Core預設環境因缺少這些 adapter依賴而 skip，不代表
  真實 CBZ／raster oracle已驗證。不得為此把 adapter依賴加入 Core。
- `.githooks/pre-merge-commit` 透過 `scripts/release-gate.py run --index`
  驗證 staged candidate；不得另建競爭的第二套 merge gate。
- release gate先離線驗證 exact candidate code review evidence，再驗證
  task-level version與 dependency audit，最後呼叫
  `scripts/check-full.sh`。成功 receipt存在 Git metadata，且只對 exact tree、
  project version、gate profile與 required-check set有效。
- release receipt只證明 bounded pytest merge profile；不得宣稱它執行或證明
  `deep` matrices、非 smoke live-MariaDB cases或 deep TLC。手動 deep結果必須
  連同 exact invocation另行回報。
- exact-tree release receipt不得 commit、修改或偽造。相同 tree的 merge
  commit與 pre-push可以重用 receipt；tree或 required checks改變就必須重跑。
- dependency audit可連網；commit hooks只驗證 candidate內既有 evidence，
  不在一般 commit過程連網。
- GitHub Actions只保留 trusted publishing、手動 formal profile、平台特有
  或本機無法可靠重現的檢查。Windows Job Object、console break、強制parent
  exit、真實descendant cleanup、venv redirector與multi-phase handoff由獨立
  `windows-latest` target驗證。
- 不使用 Claude、Codex或其他 provider-specific Stop hooks重複檢查。

## Code Review Rules

- 每次 task合併前，`scripts/git-flow-merge.sh` 必須先準備真正的 two-parent
  merge candidate，再於 Git hooks之外呼叫 `scripts/review-code.py run --index`。
  此步驟可使用已登入的 Codex服務；需要 local `.venv`與 PATH中的 `codex`。
  Review採 read-only sandbox，不得修改檔案、執行 merge/gate、啟動服務或
  自動修正問題。不得要求 GitHub PR、Actions或 provider-specific Stop hook。
  本機 online review目前限 POSIX，以獨立 process group在逾時或中斷時清理
  Codex及其一般 descendants；刻意脫離 group者不在保證內。Windows仍可離線
  verify，online review須先實作並驗證 Job Object生命週期；不影響 Core支援。
- 審查 primary parent到 candidate tree的完整變更，並檢查相關未修改程式碼
  的互動。完整閱讀本檔，引用既有架構與交易政策；不另建或複製政策來源。
  特別檢查 transaction boundaries、exact fencing、bounded operations與
  cleanup reachability、manifest/runtime一致性及能重現缺陷的測試缺口。
- 只回報有具體觸發條件與影響的 P0、P1、P2問題，附檔案、行號及理由。
  格式偏好交由既有 lint處理；不得為保持相容而違反本檔的品質優先原則。
  合法且已授權的破壞性變更本身不是缺陷，但遺漏資料轉換或錯誤的版本判定
  仍須回報。無足夠上下文或工具時回報 `incomplete`，不得聲稱通過。
- 任何 finding、`incomplete`、逾時、非零退出或無效輸出都阻止 merge。
  修正問題後重新執行合併流程，不得忽略 finding或手寫 passing receipt。
  Review預設 900秒；手動 `run --timeout-seconds`接受 1..3600秒。
- `review-code.py verify`只離線驗證紀錄，絕不啟動 Codex。紀錄存在 Git
  common metadata的 `h2hdb-review`，綁定 candidate tree、ordered parents、
  本檔及 reviewer實作；變更後須重新審查。支援 `--revision`對既有 commit
  執行或驗證審查；正式合併必須使用 `--index`。明確重新執行同一 candidate
  的 `run`會先廢止舊紀錄，失敗不得沿用舊結果。
  同一 candidate同時只允許一個 `run`；重疊請求在啟動審查前拒絕，不取代
  正在執行者，也不代表新的審查結果。POSIX離線 verify在審查執行中亦拒絕。
- Code review紀錄與測試 release receipt分開。既有 release gate在重用
  release receipt前及完整 checks後都必須驗證 code review，且不得自行連線
  呼叫模型。AI未發現問題不等於程式正確性的證明；本機紀錄亦非防竄改簽章。
  Codex原始結果與診斷保留於 metadata，CLI版本與 model override記錄於
  receipt；未指定 model時沿用 Codex設定，不聲稱知道 CLI未回報的解析後模型。

## 測試與例外

- runtime行為變更必須新增或更新測試；bug fix必須有 regression test。
- 新功能涵蓋正常、邊界與錯誤路徑。
- 數值測試固定隨機種子；容許誤差需有依據。
- flaky test視為失敗，不得以重跑掩蓋。
- 不設定跨 repository的統一 coverage百分比。
- plain `pytest`預設選擇 `not deep`並使用 bounded auto-xdist，且不得自行啟動
  live service；這個直接入口沒有 aggregate wall-clock deadline，需要強制
  五分鐘上限時必須使用 `scripts/run-pytest.py merge`。高成本測試檔與
  live-MariaDB cases由 collection分類為
  `deep`；只有經明確審核的
  `merge_smoke`與 `mariadb_smoke`代表性案例可分別豁免對應分類。兩個豁免互不
  隱含；同時屬於高成本檔與 MariaDB的案例必須同時明列兩者。
- live account、network、production或 destructive probe不得進入 hooks、
  一般 pytest或自動 merge gate。
- `skip` 或 `xfail` 必須有理由；`xfail` 原則上使用 `strict=True`。
- 不得為通過檢查而全域放寬工具設定。

## 完成回報

最終回覆必須包含：

- 明確說明本次是否在不考慮最小修改、也不以向後相容為限制的條件下，
  遵守程式碼品質優先要求，並提供具體依據。
- 明確判定本次是「不向後相容」或「品質優先後恰好向後相容」，說明受
  影響的介面、行為或資料格式及判定依據；不得只回覆「相容修正」或
  版本分類。若使用者明確要求保留相容，須如實說明此限制，不得稱為
  「恰好相容」。純文件或政策變更須說明沒有執行期介面或資料格式變更。
- 若不向後相容，進一步明確判定「可提供一次性相容性轉換工具」或
  「無法提供一次性相容性轉換工具」，並說明理由。尚未確認可行性時，
  須明示待確認事項，不得把未調查視為不可行。
- 轉換工具可行時，分開回報「可提供」與「已提供」；在已授權範圍內提供
  並驗證工具，附位置、使用方式、適用範圍及限制。尚未提供時須說明
  原因與待辦，不得宣稱相容性問題已解決。
- 無法提供轉換工具時，列出資料影響與確切的人工處理、刪除或重建範圍，
  並明示可保留的項目；不得只籠統要求「全部重建」。
- 實作及公開行為變化。
- 移除的 compatibility path。
- project version、version impact與 dependency audit結果。
- commits與完整檢查結果。
- primary branch與 merge commit。
- branch/worktree是否已清除。
- 是否仍未 push、publish或 deploy。

## Repository-specific scope

`h2hdb` 是 SQLite/MariaDB database、coordination與 revision catalog core。
本 repository擁有 greenfield schema epoch、normalized catalog與 operational
relations、bounded transactions、durable coordination state，以及 public
application facades。

- Core不得依賴 Pillow、FastAPI、OPDS types、`hbrowser`、filesystem scanning、
  gallery parsing或具體 archive/artwork/object-storage行為。Core只保存 opaque
  storage object、acquisition與presentation descriptors；CBZ及artwork bytes的
  render、store、resolve與release責任屬於 ingest integration及其 adapters。
- Consumers只使用 `VNextDatabaseAdminFacade`、`VNextCatalogFacade`、
  `VNextIngestFacade`、`VNextDownloadQueueFacade`與公開 immutable values，
  不得直接使用 connector、repository、generated schema或 table internals。
- 不得重新加入 `H2HDB`、`MigrationRunner`、numbered migration ledger、legacy
  hand-written catalog repositories、compatibility view或 dual-write path。
- Public administration與 catalog-opening entry points只能使用 wheel-resident
  generated schema provider。不得加入 caller-injected provider作為第二個
  schema-authoring surface或 production test seam。
- 易漂移的 relation與 bootstrap row數量不寫死在本檔；manifest、generator與
  executable checks才是 authoritative evidence。

## Manifest-first schema workflow

Logical authoring surfaces為 `verification/schema/catalog.toml`與
`verification/schema/operational.toml`。它們對宣告的 functional dependencies
採 closed-world解讀；遺漏 semantic dependency即使 checker通過仍代表設計
主張無效。

Schema變更依序進行：

1. 在 logical manifests加入或修改所有 relation、key、functional dependency、
   decomposition、bootstrap fact、semantic obligation與 materialization理由。
2. 由 repository generators重新產生 `physical.toml`、
   `operational_physical.toml`與 catalog/operational Lean schema files。
3. 重新產生 wheel-resident `_generated_vnext_schema.py` provider artifact。
4. 實作或更新 manifests指定的 validators、writer bindings、repositories與
   fault/integration evidence。
5. 在使用新 manifest前跑 schema、Lean、coverage metadata、runtime與兩個
   database backends的 checks。

- 不得手改 generated physical manifests、generated Lean schema files或
  generated runtime provider。
- relation shape變更必須同步反映在 manifests、checks與長期架構文件。
- BCNF與 physical width是不同 gates。一般 physical `catalog_*` base table只
  包含 semantic primary key與最多一個 atomic non-key value；例外的完整
  wide shape必須由 manifest明確審核。
- 不得把普通值藏入 primary key、packed scalar、JSON或 EAV，只為了讓 table
  width或 relation count看起來更小。

## Formal verification

- `verification/invariants.toml` 是 catalog與 operational contracts中所有
  `semantic_obligation` ID的 closed-world evidence index。
- 新 obligation必須依性質提供真實的 FD、Lean、TLA+、runtime refinement、
  fault或 integration evidence；gate拒絕 missing IDs、stale symbols與把有限
  TLC探索描述為無界證明的主張。
- `coverage --validate-only` 驗證 evidence contract並回報 production blockers；
  plain `coverage` 才是 strict production-readiness gate。不得把 schema
  generation、Lean或 metadata validation的成功誤稱為 production coverage。
- TLC只窮舉所選有限 constants的 reachable states。Lean theorem只對明列的
  mathematical inputs與 assumptions無界成立，不自行證明 Python、SQL、
  transaction或 filesystem effects refine模型。
- 效能調查先明列成本單位、輸入維度、預期界限與反例，再作優化結論。
  快取或分頁成本測試須包含容量前、容量上、容量後及重複循環；成本模型須
  與實際執行計數對照，並用故意退化的實作驗證測試能拒絕它。
  形式模型成立、有限實作案例通過、效能目標達成與部署環境實測是不同結論，
  必須分開回報。已知違反的目標不得改寫為已達成；未執行、skip或缺少必要
  尺度的證據不得當作調查完成。模型不得僅以假設包裝其聲稱證明的成本界限。
- Ingest效能驗收使用手動 `scripts/check-ingest-database-performance.py`，
  分別量測新增與replacement/retirement的完整工作；MariaDB須明確opt-in。
  跨adapter的結論另使用明確提供的ingest checkout內source與library cleanup
  cost驗收，不得從Core的neutral bytes推論圖片或filesystem成本。這些結果與
  bounded merge receipt分開回報。量測完成、成本達標與全庫工期達標是不同
  結果；exit 0代表所選成本契約滿足，1代表違反，2代表證據不足或執行失敗。
  工具正確性測試可驗證目前違反會被拒絕，但其通過不得覆蓋失敗的成本結果。
  後續效能修改須以事先固定的預算及退化反例驗證所宣稱解決的目標，不得由
  當次baseline/candidate的平均產生驗收上限，也不得為通過而調高預算。

## Architecture and transaction rules

- Protocols放在 `ports.py`；neutral data放在 `domain.py`。
- Backend-specific行為留在 `SQLConnector`後方。共用 SQL使用 `%s`，SQLite
  connector負責轉成 `?`。
- 每個 cross-table workflow共享單一 connector與 managed transaction。
  Internal unit-of-work可以協調 repositories；consumer不得呼叫 internal API。
- SQLite writes使用 `BEGIN IMMEDIATE`；MariaDB在 fencing、allocation或 epoch
  serialization需要時使用 row/advisory locks。read-only mode由 connector
  強制。
- 不得信任 caller提供的 digest、derived ID、count、cursor、name、generation、
  lease或 token。必須重新計算或載入 durable authority，mismatch時 fail closed。
- 任意長資料使用 bounded canonical pages與 streaming validation。Batch必須
  hard-capped、keyset-paged、idempotent且能承受 response loss。
- Immutable identity、history、event、receipt與 publication facts不得原地
  update；mutable state隔離於 normalized heads、owners、leases、checkpoints與
  明確 state-machine relations。
- Publication與 coordinated completion只在短 transaction內驗證 sealed scalar
  state，不得掃描無界 source、projection、artifact、queue或 event集合。
- Exact attempt/generation/token fencing必須阻止 delayed retry完成或修改替代
  work。
- Cleanup必須 bounded、child-first且檢查 reachability；任何仍被 active work、
  publication、pending effects或 protection claims引用的 identity/history都須
  保留。
- Catalog calls只接受 current publication head。 supplied `CatalogRevision`只在
  仍精確等於該 head時有效；每次 read完成 pinned snapshot後，回傳前都須在
  第二個 fresh read transaction精確重查 head。
- 每個 published catalog occurrence必須有一筆 immutable
  `catalog_publication_download_time` child，保存被選 observation的
  `download_time`；publication seal、READY audit與 cleanup都必須證明或維持
  這個 total authority，不得從 mutable downloader queue推導。
- `CatalogReader.discover_publications`只可使用 revision-scoped normalized SQL
  discovery authority。Nonblank search使用 pinned Unicode policy產生的 lexeme
  AND matching；title以 `catalog_title_search_postings`中的 display/source title
  lexeme subset限定欄位。GID精確匹配、language、subjects與contributor為exact
  filters；多subject條件全部AND，uploaded/downloaded為UTC半開時間區間，pages
  為sealed artifact實際頁數的包含兩端區間。不得以source page count或mutable
  downloader queue代替published authority；沒有artifact的項目不符合pages條件。
  所有條件共同參與canonical query digest與filtered-set membership驗證。
  結果使用hard-capped keyset pages；caller cursor必須重新驗證revision、query
  digest、position與filtered-set membership，不得以hydrate後的Python scan取代index。
- `CatalogReader.list_publication_facets`對language、subject與contributor提供
  keyset-paged exact publication counts。計算某facet family時忽略同family既選
  filter，但保留search及其他family filters；cursor同樣不得作為authority。
- `CatalogReader.list_recent_publications`提供 current revision中具有 sealed
  acquisition descriptor之publications的固定完整 top 128 window，不接受 caller
  `limit`或 cursor。`UPLOADED`依 `upload_time DESC, gid DESC`，`DOWNLOADED`依
  published `download_time DESC, gid DESC`；沒有acquisition的revision回傳空
  window。
- `CatalogReader.list_tag_values`依 exact namespace列出tag值，依各tag的current
  publications最新uploaded time降序、exact UTF-8值bytes升序。`list_tag_publications`
  接受 `CatalogTagFilter`，依uploaded time降序、既有casefolded UTF-8 display-title
  sort bytes升序、publication identity升序。Namespace與值接受source的完整值域
  （UTF-8 namespace 0..128 bytes、value 0..65536 bytes），不套用search facet的較窄
  query限制。兩者只讀sealed revision-scoped `tag_directory_order`與
  `tag_publication_order`，每頁最多128筆並以position keyset接續；caller cursor必須
  精確重驗revision、namespace或exact tag、position與membership，禁止request-time
  全量GROUP BY/MAX、sort或hydrate後Python scan。Preparation以disk plan一次排序，
  bounded publication child batches獨立exact-compare後seal，READY獨立重建驗證。
- `CatalogReader.list_tag_values_with_publications`在同一pinned snapshot回傳tag
  page與逐項對應的第一筆publication；只選各tag既有order的position 0，不因缺少
  acquisition或presentation而跳過。每頁最多128筆，去重publication keys後批次
  hydrate，再依tag順序對齊；禁止每tag另做public read或全量掃描。
- Acquisition與presentation只保存 neutral immutable descriptors。Core不得
  指定hash path、CBZ/ZIP layout或共享mount；ingest adapters擁有archive與
  artwork bytes及其storage lifecycle，且byte I/O不得進入core DB transaction。
- 在 manifest定義 durable revision-scoped authority與 replay semantics前，
  不得從 transient joins推導 `redownload_required`。
- Public ingest orchestration把 transaction-owned issue/commit與 adapter-owned
  local preparation分開；filesystem與 object-storage不得進入 core DB
  transaction。
- 每個 sealed source observation的 METADATA codec v2包含 immutable qualification
  policy digest與 accepted/rejected結果。Core以 normalized children exact-compare
  canonical metadata；rejected結果必須指向該 observation中一個 sealed PAGE。
  Source snapshot保留所有 observations與 FILE facts，analysis的spam、content、
  GID選擇與overlay只使用accepted observations。不得逐頁丟棄後假裝整本有效。
  Marker reuse同時驗證producer完成證據與目前artifact policy/required mode；
  changed marker或policy須重新觀測。Cleanup依source/analysis reachability保留
  qualification children，無引用時bounded child-first回收。圖片解碼與resource
  errors分類由adapter負責，不得聲稱SQL qualification證明了外部bytes可解碼。

## Schema epoch and backend rules

- 只有本 repository擁有 schema。CLI對 epoch 3/schema version 8只公開 `migrate`、
  `check`與 `ready`。
- `migrate`只接納真正空白 database，寫入 checksum-bound `BUILDING` marker，
  套用 idempotent generated DDL/bootstrap slices，驗證 exact manifests後轉為
  `READY`。
- Interrupted run只可恢復相同 manifest-bound `BUILDING` epoch；`READY`重跑
  `migrate`/`initialize()`只做固定成本、read-only control shape與
  epoch/version/manifest marker admission，回報 `already_ready`，不得宣稱
  完整 audit。只有 created/resumed結果含本次完整 activation audit。
- BUILDING初始化保留每個 generated slice的 exact object/shape驗證與最終
  semantic驗證前後的 fresh closed-world inventory；不得信任跨次執行快取。
- Previous、foreign或 malformed control必須拒絕，database error不得視為
  empty；READY marker admission不證明 data-plane完整，schema/data drift由
  明確 `check`完整拒絕。Ingest啟動使用core持久化的稽核排程，依正常結束、
  最近完整稽核、validator版本與到期狀態選擇快速或完整檢查；排程紀錄不是
  後續資料完整性的證明。其他caller須明確選擇check或readiness，不得將
  quick結果宣稱為full audit；公開open_database仍保留完整check契約。
- Ingest runtime稽核排程是明確mutable operational state；core執行完整
  check成功且fresh generation/token重驗後才能更新完整稽核時間。Caller
  不得傳入成功旗標作為依據。首次source catch-up提示只能延後一次排程，
  不得更改last audit事實；正常結束只在工作與所有資源清理成功後記錄。
- 已完成的schema7至8離線轉換工具、Docker bundle builder與專用audit writer
  已自目前checkout移除，不保留shim或runtime fallback。尚未轉換的exact schema7
  或該工具留下的中斷狀態，須使用Core0.41.2歷史checkout與匹配環境；schema6
  須先用Core0.40.0歷史工具轉至schema7。已完成轉換的schema8不需再遷移、清庫
  或重建CBZ。Previous、foreign與離線轉換中的marker仍不由runtime接納。
- 每個 production SQL relation identifier都必須由 `physical.toml`、
  `operational_physical.toml`或唯一 epoch-control relation接納。Formal BCNF通過
  不代表可以發布第二套未 manifest的 SQL schema。
- `check`在 read transaction執行完整 `READY` audit；`ready`是 O(1) read-only
  epoch/version/manifest probe。Provider blocker必須在開啟或修改 database前
  fail；consumers不得初始化 schema。
- Shared schema、transaction、connector、validator或 repository變更都須測
  SQLite。MariaDB cases以 `H2HDB_TEST_MARIADB=1`啟用 testcontainers，並固定
  MariaDB 10.11.11，對應 Synology package build 10.11.11-1551。
- bounded merge profile只執行明列的少量 `mariadb_smoke`；其餘 MariaDB cases
  屬於手動 deep profile，不得由 release receipt推稱已執行。
- Docker不可用時必須精確回報；不得把未執行 MariaDB測試描述為通過。
