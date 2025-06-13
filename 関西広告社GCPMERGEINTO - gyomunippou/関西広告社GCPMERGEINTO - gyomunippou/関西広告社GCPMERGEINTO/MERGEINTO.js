require('dotenv').config();
const express = require('express');
const sql = require('mssql');
const { Connector } = require('@google-cloud/cloud-sql-connector');
const { KintoneRestAPIClient } = require('@kintone/rest-api-client');

const app = express();
const PORT = process.env.PORT || 8080;

// 環境変数の取得
const {
  DB_USER,
  DB_PASSWORD,
  INSTANCE_CONNECTION_NAME,
  DB_NAME,         // SQLのターゲットテーブル名（例："MyTable"）
  KINTONE_DOMAIN,
  KINTONE_APP_ID,
  KINTONE_API_TOKEN,
  SQL_TABLE_Base,      // SQLのターゲットテーブル名（例："MyTable"）
  SQL_TABLE_Detail,    // SQLのターゲットテーブル名（例："MyTable"）
} = process.env;

if (!DB_USER || !DB_PASSWORD || !INSTANCE_CONNECTION_NAME || !DB_NAME || !SQL_TABLE_Base ||!SQL_TABLE_Detail || !KINTONE_DOMAIN || !KINTONE_APP_ID || !KINTONE_API_TOKEN) {
  console.error('必要な環境変数が設定されていません。');
  process.exit(1);
}

// Kintoneの基本URLとクライアントの設定
const kintoneBaseUrl = `https://${KINTONE_DOMAIN}.cybozu.com/`;
const kintoneClient = new KintoneRestAPIClient({
  baseUrl: kintoneBaseUrl,
  auth: { apiToken: KINTONE_API_TOKEN },
});

// SQL接続用のグローバル変数
let dbPool = null;

// Cloud SQL（SQL Server）の接続を作成する関数
const createSqlConnection = async () => {
  const connector = new Connector();
  const clientOpts = await connector.getTediousOptions({
    instanceConnectionName: INSTANCE_CONNECTION_NAME,
    ipType: 'PUBLIC', // または 'PRIVATE'
  });
  
  const config = {
    user: DB_USER,
    password: DB_PASSWORD,
    options: {
      ...clientOpts,
      database: DB_NAME,
    },
  };
  // 接続先のサーバーIPまたはホスト名（必要に応じて修正）
  config.server = '35.200.74.95';

  const pool = new sql.ConnectionPool(config);
  try {
    await pool.connect();
    console.log("Cloud SQLへの接続に成功しました。");
    return pool;
  } catch (err) {
    console.error("Cloud SQLへの接続に失敗しました:", err);
    connector.close();
    throw err;
  }
};

// SQLクエリ実行用の汎用関数
const executeQuery = async (sqlQuery, parameters = []) => {
  try {
    if (!dbPool) {
      dbPool = await createSqlConnection();
    }
    const request = dbPool.request();
    parameters.forEach(param => {
      request.input(param.name, param.type, param.value);
    });
    const result = await request.query(sqlQuery);
    return result.recordset;
  } catch (error) {
    console.error('クエリ実行エラー:', error);
    return [];
  }
};

// Kintoneから対象レコードを全件取得する関数
const getRecordsFromKintone = async () => {
  try {
    const records = await kintoneClient.record.getAllRecords({
      app: KINTONE_APP_ID,
      // 必要に応じてqueryやfieldsの指定も可能
    });
    console.log(`取得したKintoneレコード件数: ${records.length}`);
    return records;
  } catch (error) {
    console.error('Kintoneからレコード取得エラー:', error);
    return [];
  }
};

// Kintoneの1レコードをSQLにアップサート（存在すれば更新、なければ挿入）する関数
const upsertRecordBaseSQL = async (record) => {
  const npidx        = record.$id?.value;
  const koujibi    = record.実施日?.value;
  const member     = record.社員CD?.value;
  const shoninnsha = record.承認者CD?.value;
  const shouninbi  = record.承認日?.value;
  const kessaisha  = record.決裁者CD?.value;
  const kessaibi   = record.決裁日?.value;

  // SQL Serverの場合、MERGE文を使用したアップサート例
  const sqlQuery = `
MERGE INTO ${SQL_TABLE_Base} AS target
USING (
  VALUES (@Npidx, @Koujibi, @Member, @Shoninnsha, @Shouninbi, @Kessaisha, @Kessaibi)
) AS source (npidx, koujibi, member, shoninnsha, shouninbi, kessaisha, kessaibi)
ON target.npidx = source.npidx
WHEN MATCHED THEN
  UPDATE SET koujibi = source.koujibi, member = source.member, shoninnsha = source.shoninnsha, shouninbi = source.shouninbi, kessaisha = source.kessaisha, kessaibi = source.kessaibi
WHEN NOT MATCHED THEN
  INSERT (npidx, koujibi, member, shoninnsha, shouninbi, kessaisha, kessaibi)
  VALUES (source.npidx, source.koujibi, source.member, source.shoninnsha, source.shouninbi, source.kessaisha, source.kessaibi);
  `;
  try {
    await executeQuery(sqlQuery, [
      { name: 'Npidx', type: sql.Int, value: npidx },
      { name: 'Koujibi', type: sql.DateTime, value: koujibi },
      { name: 'Member', type: sql.VarChar, value: member },
      { name: 'Shoninnsha', type: sql.VarChar, value: shoninnsha },
      { name: 'Shouninbi', type: sql.DateTime, value: shouninbi },
      { name: 'Kessaisha', type: sql.VarChar, value: kessaisha },
      { name: 'Kessaibi', type: sql.DateTime, value: kessaibi },
    ]);
    console.log(`レコードID: ${npidx} をSQLにアップサートしました。`);
  } catch (error) {
    console.error(`レコードID: ${npidx} のアップサート中にエラーが発生しました。`, error);
  }
};
const upsertRecordDetailSQL = async (record) => {
  const ndidx    = record.$id?.value;
  const npidx                 = record.$id?.value;
const rawKikakuNo = record['企画No']?.value;

const kikaku_no = (rawKikakuNo === '内作・その他')
  ? '0'.repeat(9)     // → "000000000"
  : (rawKikakuNo || null);
  const kikaku_seko_record_no = record['KIKAKU_SEKO_RECORD_NO']?.value || null;
  const keisu                 = record.係数?.value                   || null;
  const naiyo                 = record.作業No?.value               || null;
  const quant                 = record.数値?.value                   || null;
  const depth                 = record.深さ?.value                   || null;
  const diam                  = record.径?.value                    || null;
  const bubun                 = record.部分書換?.value              || null;
  const hsakusei              = record.オプション?.value             || null;
  const biko                  = record.備考?.value                   || null;
  const starttime             = record.作成日時?.value                   || null;
  const finishtime            = record.作成日時?.value                   || null;


  // SQL Serverの場合、MERGE文を使用したアップサート例
  const sqlQuery = `
MERGE INTO ${SQL_TABLE_Detail} AS target
USING (
  VALUES (
    @Ndidx,
    @Npidx,
    @Kikaku_no,
    @Kikaku_seko_record_no,
    @Keisu,
    @Naiyo,
    @Quant,
    @Depth,
    @Diam,
    @Bubun,
    @Hsakusei,
    @Biko,
    @Starttime,
    @Finishtime
  )
) AS source (
  ndidx,
  npidx,
  kikaku_no,
  kikaku_seko_record_no,
  keisu,
  naiyo,
  quant,
  depth,
  diam,
  bubun,
  hsakusei,
  biko,
  starttime,
  finishtime
)
ON target.ndidx = source.ndidx
WHEN MATCHED THEN
  UPDATE SET
    kikaku_no             = source.kikaku_no,
    kikaku_seko_record_no = source.kikaku_seko_record_no,
    keisu                 = source.keisu,
    naiyo                 = source.naiyo,
    quant                 = source.quant,
    depth                 = source.depth,
    diam                  = source.diam,
    bubun                 = source.bubun,
    hsakusei              = source.hsakusei,
    biko                  = source.biko,
    starttime             = source.starttime,
    finishtime            = source.finishtime
WHEN NOT MATCHED THEN
  INSERT (
    ndidx,
    npidx,
    kikaku_no,
    kikaku_seko_record_no,
    keisu,
    naiyo,
    quant,
    depth,
    diam,
    bubun,
    hsakusei,
    biko,
    starttime,
    finishtime
  )
  VALUES (
    source.ndidx,
    source.npidx,
    source.kikaku_no,
    source.kikaku_seko_record_no,
    source.keisu,
    source.naiyo,
    source.quant,
    source.depth,
    source.diam,
    source.bubun,
    source.hsakusei,
    source.biko,
    source.starttime,
    source.finishtime
  );
`;


  try {
    await executeQuery(sqlQuery, [
      { name: 'Ndidx', type: sql.Int, value: ndidx },
      { name: 'Npidx', type: sql.Int, value: npidx },
      { name: 'Kikaku_no', type: sql.VarChar, value: kikaku_no },
      { name: 'Kikaku_seko_record_no', type: sql.Int, value: kikaku_seko_record_no },
      { name: 'Keisu', type: sql.Int, value: keisu },
      { name: 'Naiyo', type: sql.Int, value: naiyo },
      { name: 'Quant', type: sql.Float, value: quant },
      { name: 'Depth', type: sql.Float, value: depth },
      { name: 'Diam', type: sql.Float, value: diam },
      { name: 'Bubun', type: sql.Int, value: bubun },
      { name: 'Hsakusei', type: sql.Int, value: hsakusei },
      { name: 'Biko', type: sql.VarChar, value: biko },
      { name: 'Starttime', type: sql.DateTime, value: starttime },
      { name: 'Finishtime',type: sql.DateTime, value: finishtime },
    ]);
    console.log(`レコードID: ${ndidx} をSQLにアップサートしました。`);
  } catch (error) {
    console.error(`レコードID: ${ndidx} のアップサート中にエラーが発生しました。`, error);
  }
};

// メイン処理：Kintoneから取得した全レコードをループしてSQLに取り込む
const main = async () => {
  try {
    const kintoneRecords = await getRecordsFromKintone();
    for (const record of kintoneRecords) {
      await upsertRecordBaseSQL(record);
      await upsertRecordDetailSQL(record);
    }
  } catch (error) {
    console.error('メイン処理中にエラーが発生しました。', error);
  } finally {
    if (dbPool) {
      try {
        await dbPool.close();
        console.log('データベース接続をクローズしました。');
      } catch (closeError) {
        console.error('データベース接続クローズ時にエラーが発生:', closeError);
      }
    }
  }
};

// エンドポイント：HTTPリクエストで同期処理をトリガー
app.get('/', async (req, res) => {
  try {
    // エンドポイント呼び出し時にSQL接続を作成
    dbPool = await createSqlConnection();
    await main();
    res.send('KintoneからSQLへのデータ同期が完了しました。');
  } catch (error) {
    console.error('エンドポイント処理中にエラーが発生しました。', error);
    res.status(500).send(`エラーが発生しました: ${error.message}`);
  } finally {
    if (dbPool) {
      try {
        await dbPool.close();
        console.log('データベース接続をクローズしました。');
      } catch (closeError) {
        console.error('データベース接続クローズ時にエラーが発生:', closeError);
      }
    }
  }
});

// サーバー起動
app.listen(PORT, () => {
  console.log(`サーバーがポート ${PORT} で起動しました。`);
});
