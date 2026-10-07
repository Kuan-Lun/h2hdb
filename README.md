# h2hdb

`h2hdb` 是 H2HDB 書庫共用的資料庫套件，保存書目、匯入進度與下載工作，
支援 SQLite 和 MariaDB。你可以用它建立、檢查資料庫，或從 Python 查詢書目。

如果你想建立可閱讀的書庫，請依需求選擇應用程式：

| 你想做的事 | 使用的專案 |
| --- | --- |
| 將已下載的圖庫匯入書庫，產生 CBZ、封面與縮圖 | [h2hdb-ingest](https://github.com/Kuan-Lun/h2hdb-ingest) |
| 用 OPDS 閱讀器瀏覽、搜尋及下載書籍 | [h2hdb-opds](https://github.com/Kuan-Lun/h2hdb-opds) |
| 將已發布書庫同步至 Komga | [h2hdb-komga](https://github.com/Kuan-Lun/h2hdb-komga) |
| 處理圖庫下載工作 | [h2hdb-downloader](https://github.com/Kuan-Lun/h2hdb-downloader) |

這些應用程式使用同一個資料庫。單獨安裝 `h2hdb` 不會匯入檔案、產生 CBZ
或啟動網頁服務。若你只是要使用其中一項服務，先依該專案的 README 安裝；
本頁適用於管理共用資料庫與撰寫 Python 整合程式。

## 安裝

需要 Python 3.14 以上。SQLite 不需另裝資料庫伺服器；MariaDB 的驗證基準為
10.11.11，包括 Synology 套件版本 10.11.11-1551。

取得本專案原始碼後，在專案根目錄建立虛擬環境並安裝：

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install .
python -m h2hdb --help
```

Windows PowerShell 使用 `py -3.14 -m venv .venv` 建立環境，
再以 `.venv\Scripts\Activate.ps1` 啟用。下文的 `python` 指令都在此環境執行。

如果環境已由其他 H2HDB 應用程式管理，使用它安裝的 Core 版本即可。
升級共用部署時，各應用程式的相依版本範圍必須能選出同一個 Core 版本，
且該版本必須支援既有資料庫的 schema。

## 快速開始：SQLite

將以下內容存成 `core-writer.json`：

```json
{
  "database": {
    "sql_type": "sqlite",
    "database": "catalog.sqlite3",
    "access_mode": "read-write"
  },
  "logger": {
    "level": "INFO",
    "file": null
  }
}
```

建立空白資料庫並檢查：

```bash
python -m h2hdb migrate --config core-writer.json
python -m h2hdb check --config core-writer.json
```

首次建立成功會顯示 `outcome=created`、`state=READY`。
此時資料庫可供 ingest 使用，但還沒有已發布書目；接著依
[ingest 使用說明](https://github.com/Kuan-Lun/h2hdb-ingest)匯入圖庫。

範例將資料庫放在**執行指令時的工作目錄**。用於常駐服務時，請改成持久儲存
目錄的絕對路徑，先建立父目錄，並讓執行帳號具有寫入權限。

## 改用 MariaDB

先在 MariaDB 伺服器建立空白資料庫與專用帳號。
初始化帳號需具備建立資料表、view、索引及讀寫資料所需的權限。
將連線設定存成 `core-writer.json`：

```json
{
  "database": {
    "sql_type": "mariadb",
    "host": "127.0.0.1",
    "port": 3306,
    "user": "h2hdb_writer",
    "password": "${H2HDB_DB_PASSWORD}",
    "database": "h2hdb",
    "access_mode": "read-write"
  },
  "logger": {
    "level": "INFO",
    "file": null
  }
}
```

在執行環境設定 `H2HDB_DB_PASSWORD`，再執行同樣的 `migrate`、`check` 指令。
`migrate` 只在指定資料庫內建立 schema，不會安裝 MariaDB、建立資料庫或帳號。

### 唯讀帳號與設定檔

查詢書目或執行檢查時，可複製設定為 `core-reader.json`，
將 `access_mode` 改成 `read-only`。MariaDB 也應改用具備讀取資料與檢查 schema
所需權限的專用唯讀帳號；寫入帳號留給 ingest、downloader 與資料庫管理工作。

設定值若完整寫成 `${ENV_NAME}`，會以環境變數取代；變數不存在時會拒絕啟動。
`"library-${INSTANCE}"` 這類字串內插不會展開。未知欄位也會被拒絕。

Core 設定包含 `database`、可省略的 `logger` 與 `maintenance`。
一般安裝可沿用 maintenance 預設值；完整欄位與預設值見
[設定定義](src/h2hdb/config_loader.py)。其他應用程式可能把 Core 設定包在
另一層 JSON 中，請使用該應用程式的範例，不要直接互換設定檔。

## 日常檢查與紀錄

| 指令 | 適用時機 | 成功代表什麼 |
| --- | --- | --- |
| `python -m h2hdb migrate --config core-writer.json` | 首次建立，或接續中斷的初始化 | 已建立或接續完成；若已初始化，只確認既有 READY 標記 |
| `python -m h2hdb check --config core-reader.json` | 升級後、懷疑資料異常，或需要完整稽核 | schema、書目與工作狀態通過完整資料庫檢查 |
| `python -m h2hdb ready --config core-reader.json` | 頻繁執行的服務就緒探測 | READY 標記、schema 版本與 manifest 符合目前套件 |

`ready` 是快速唯讀探測，不代表已檢查全部資料。
`check` 會完整稽核，大型書庫可能需要較久；兩者都不會驗證外部 CBZ 或圖片內容。

對已初始化的資料庫重跑 `migrate` 會顯示 `outcome=already_ready`、
`audit=not_performed`；需要完整檢查時仍應執行 `check`。
初始化若中斷，使用相同版本與設定重跑 `migrate`，只能接續相符的未完成初始化。

Ingest 自行安排啟動與定期稽核。非正常停止、升級 Core 或稽核到期後，
啟動可能需要完整檢查；快速啟動不表示剛完成一次完整稽核。

紀錄預設輸出至終端。設定 `logger.file` 可另存檔案；
需要更詳細的診斷時，將 `logger.level` 改為 `DEBUG`。
長時間工作的進度紀錄可幫助辨識目前階段，仍需等到成功結果才能認定工作完成。
診斷中的 SQL 呼叫數、回傳列數與耗時，不能直接當成伺服器掃描列數或磁碟 I/O。

## 升級與還原

目前使用 **epoch 3、schema 9**。`migrate` 是初始化指令，
**不會自動升級舊資料庫，也不是損毀修復工具**。
更換應用程式組合前，請備份資料庫及與它配套的 library 儲存內容。

| 既有資料庫 | 處理方式 |
| --- | --- |
| schema 9 | 不需轉換 schema、清庫或重新產生 CBZ／artwork；確認應用程式版本相容並執行 `check` |
| schema 8，或其歷史轉換工具留下的中斷狀態 | 使用 Core 0.43.0 歷史 checkout 與匹配環境的離線工具轉成 schema 9 |
| schema 7 | 先使用 Core 0.41.2 歷史工具轉成 schema 8，再轉成 schema 9 |
| schema 6 | 先使用 Core 0.40.0 歷史工具轉成 schema 7，再依序轉換 |
| schema 5 或更早 | 沒有支援的原地轉換；保留原資料庫與原始下載圖庫，在新的空白資料庫和 library 重新匯入 |

歷史轉換工具已從目前 checkout 移除。請使用對應歷史版本的 README、wheel
與執行環境，不要混用工具檔案或手動更改 schema 標記：

- schema 8 → 9：Core 0.43.0，commit
  `70ca4a35d02a50e7d6f8fd294fccb0829321eaf4`；
  其隔離 cleanup worker 另需該版本 README 指定的 Core 0.42.2 wheel。
- schema 7 → 8：Core 0.41.2，commit
  `64683c502caf108e8ed518e5c040cacaa91e7551`。

轉換期間保持 consumers 停止，等完整稽核與 schema 9 啟用成功再恢復服務。
這些歷史轉換可保留資料庫內容與外部媒體；中斷後依對應工具的恢復說明處理。
若改用新庫重建，原本的下載請求與作業歷史不會自動恢復，仍需要的請求須重新加入。
在新書庫驗證完成前，保留舊資料庫與 library 配對。需要退回舊軟體時，
還原相應的升級前備份；沒有自動降版功能。

## 從 Python 查詢書目

以下範例查詢已由 ingest 發布的書庫，使用前先準備 `core-reader.json`：

```python
from contextlib import closing

from h2hdb import CatalogDiscoveryQuery, load_config, open_database

with closing(open_database(load_config("core-reader.json"))) as catalog:
    revision = catalog.get_catalog_revision()
    page = catalog.discover_publications(
        query=CatalogDiscoveryQuery(search="example title"),
        limit=50,
        revision=revision,
    )
    for publication in page.publications:
        print(publication.publication_id, publication.title)
```

`open_database()` 會先完整稽核資料庫。每頁最多 128 筆；
下一頁使用相同 query、revision，並傳入 `after=page.next_cursor`。
`next_cursor` 為 `None` 時已到最後一頁。
若 ingest 在分頁期間發布新版本，請重新取得 revision 並從第一頁開始查詢。

搜尋詞與各篩選條件以 AND 組合。語言、貢獻者和標籤使用精確值；
上傳／下載時間區間含起點、不含終點，頁數區間包含兩端，依已產生檔案的頁數篩選。

| 公開入口 | 用途 |
| --- | --- |
| `VNextDatabaseAdminFacade` | 初始化、完整檢查與就緒探測 |
| `VNextCatalogFacade` | 查詢書目、搜尋、分類、標籤、最近書籍及檔案描述 |
| `VNextDownloadQueueFacade` | 提交與管理下載請求 |
| `VNextIngestFacade` | 整合匯入流程 |

從 `h2hdb` 匯入公開 API；方法簽名可查閱
[facade 定義](src/h2hdb/vnext_facade.py)與[資料型別](src/h2hdb/domain.py)。
查詢回傳的圖片與下載項目是描述資料，實際檔案由應用程式的儲存 adapter 提供。

## 常見問題

| 狀況 | 建議檢查 |
| --- | --- |
| 設定檔讀取失敗 | JSON 語法、欄位名稱、環境變數是否存在，以及是否拿錯應用程式的設定檔 |
| SQLite 無法開啟或寫入 | 資料庫絕對路徑、父目錄與執行帳號權限 |
| MariaDB 連線或權限錯誤 | 伺服器、port、資料庫名稱、帳密與帳號權限 |
| 初始化中斷，尚未 READY | 使用相同版本與設定重跑 `migrate` |
| schema 或 manifest 不符 | 先確認套件版本與資料庫位置，再依升級說明處理 |
| `ready` 成功但書庫是空的 | 確認 ingest 已完成第一次發布；初始化本身不會匯入圖庫 |
| `check` 失敗 | 保留錯誤與備份，查明原因後再決定還原或重建 |
| Python revision 或 cursor 失效 | 重新取得目前 revision，從頭執行查詢 |

回報問題時，請在 [issue tracker](https://github.com/Kuan-Lun/h2hdb/issues)
提供套件版本、資料庫種類、執行指令與移除敏感資料後的錯誤訊息。

需要重現軟體檢查時，見[驗證指南](verification/README.md)；
需要量測查詢或匯入成本時，見[效能量測指南](benchmarks/README.md)。
這些工具使用測試環境，其結果不能取代對實際書庫執行的檢查。

## 授權

[GNU General Public License version 3](LICENSE)。
