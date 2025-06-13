require('dotenv').config();

const express = require('express');
const sql = require('mssql');
const { Connector } = require('@google-cloud/cloud-sql-connector');
const { KintoneRestAPIClient } = require('@kintone/rest-api-client');

const app = express();
const PORT = process.env.PORT || 8080;

const {
  DB_USER,
  DB_PASSWORD,
  INSTANCE_CONNECTION_NAME,
  DB_NAME,
  KINTONE_DOMAIN,
  KINTONE_APP_ID,
  KINTONE_API_TOKEN,
  SYAIN_API_TOKEN
} = process.env;

if (!DB_USER || !DB_PASSWORD || !INSTANCE_CONNECTION_NAME || !DB_NAME || !KINTONE_DOMAIN || !KINTONE_APP_ID || !KINTONE_API_TOKEN) {
  console.error('必要な環境変数が設定されていません。');
  process.exit(1);
}

const kintoneBaseUrl = `https://${KINTONE_DOMAIN}.cybozu.com/`;
const kintoneClient = new KintoneRestAPIClient({
  baseUrl: kintoneBaseUrl,
  auth: { apiToken: KINTONE_API_TOKEN },
});
const SYAINClient = new KintoneRestAPIClient({
  baseUrl: kintoneBaseUrl,
  auth: { apiToken: SYAIN_API_TOKEN },
});

// グローバル変数として dbPool を宣言
let dbPool = null;

/**
 * SQLサーバー接続を返す関数
 */
async function createSqlConnection() {
  const connector = new Connector();
  const clientOpts = await connector.getTediousOptions({
    instanceConnectionName: INSTANCE_CONNECTION_NAME,
    ipType: 'PUBLIC',
  });
    
  const config = {
    user: DB_USER,
    password: DB_PASSWORD,
    options: {
      ...clientOpts,
      database: DB_NAME,
    },
  };
  config.server = '35.200.74.95'; // 実際のサーバIP, 例: '35.200.74.95'

  const pool = new sql.ConnectionPool(config);
  try {
    await pool.connect();
    console.log('Cloud SQL への接続に成功しました。');
    return pool;
  } catch (err) {
    console.error('Cloud SQL 接続失敗:', err);
    connector.close();
    throw err;
  }
}

async function executeQuery(sqlQuery, parameters = []) {
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
}

/**
 * ① reccnt チェック用関数
 *    例: S_ReplicaDayテーブルで、reccnt>0 のレコードが
 *        直近10分以内に1件以上あれば true
 */
async function hasReccntData() {
  const query = `
    SELECT COUNT(*) as cnt
      FROM SekouSiji.dbo.S_ReplicaDay as S
     WHERE S.reccnt > 0 AND S.ReplicaDay >= DATEADD(minute, -20, GETDATE())
  `;
  const rows = await executeQuery(query);
  if (!rows || rows.length === 0) return false;
  return rows[0].cnt > 0;
}

/**
 * ② getTodayRecordsFromSql()
 *    SQLから工事日=今日の全レコードを取得する (物理削除済みなら消えている)
 */
function getTodayYYYYMMDD() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

async function getUpsertRecordsFromSql() {
  const sqlQuery = `
    select
  K.KIKAKU_NO as 企画番号,
  SI.SELECT_ITEM_NM as 区分,
  BA.BAITAI_CD as 媒体名,
  K.DESIGN_NAIYO as 内容,
  SI2.SELECT_ITEM_NM as 媒体種別,
  K.EIGYO_TANTO_CD as 営業CD,
  ES.SYAIN_NM as 営業,
  K.TODOHUKEN_CD as 都道府県CD,
  TD.TODOHUKEN_NM as 都道府県,
  K.SIKUGUN as 市区郡,
  K.SETTI_BASYO as 設置場所,
  K.BAITAI_ZAHYO as 緯度経度,
  K.BAITAI_KOSEI as 媒体構成,
  case when BM1.MEN_CD is not null then format(BM1.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM1.KEIYAKU_SIZE_WIDTH,'0')  else '' end
    + case when BM2.MEN_CD is not null then case when (BM2.KEIYAKU_SIZE_HEIGHT = BM1.KEIYAKU_SIZE_HEIGHT and BM2.KEIYAKU_SIZE_WIDTH = BM1.KEIYAKU_SIZE_WIDTH) then '' else ' ' + format(BM2.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM2.KEIYAKU_SIZE_WIDTH,'0') end else '' end
    + case when BM3.MEN_CD is not null then case when (BM3.KEIYAKU_SIZE_HEIGHT = BM1.KEIYAKU_SIZE_HEIGHT and BM3.KEIYAKU_SIZE_WIDTH = BM1.KEIYAKU_SIZE_WIDTH) then '' else ' ' + format(BM3.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM3.KEIYAKU_SIZE_WIDTH,'0') end else '' end
    + case when BM4.MEN_CD is not null then case when (BM4.KEIYAKU_SIZE_HEIGHT = BM1.KEIYAKU_SIZE_HEIGHT and BM4.KEIYAKU_SIZE_WIDTH = BM1.KEIYAKU_SIZE_WIDTH) then '' else ' ' + format(BM4.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM4.KEIYAKU_SIZE_WIDTH,'0') end else '' end
+ case when BM5.MEN_CD is not null then case when (BM5.KEIYAKU_SIZE_HEIGHT = BM1.KEIYAKU_SIZE_HEIGHT and BM5.KEIYAKU_SIZE_WIDTH = BM1.KEIYAKU_SIZE_WIDTH) then '' else ' ' + format(BM5.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM5.KEIYAKU_SIZE_WIDTH,'0') end else '' end
    + case when BM6.MEN_CD is not null then case when (BM6.KEIYAKU_SIZE_HEIGHT = BM1.KEIYAKU_SIZE_HEIGHT and BM6.KEIYAKU_SIZE_WIDTH = BM1.KEIYAKU_SIZE_WIDTH) then '' else ' ' + format(BM6.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM6.KEIYAKU_SIZE_WIDTH,'0') end else '' end as サイズ,
  round(
      cast(case when BM1.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM1.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM1.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM1.KEIYAKU_SIZE_WIDTH end as float)/1000
      + cast(case when BM2.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM2.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM2.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM2.KEIYAKU_SIZE_WIDTH end as float)/1000
      + cast(case when BM3.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM3.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM3.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM3.KEIYAKU_SIZE_WIDTH end as float)/1000
      + cast(case when BM4.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM4.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM4.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM4.KEIYAKU_SIZE_WIDTH end as float)/1000
      + cast(case when BM5.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM5.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM5.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM5.KEIYAKU_SIZE_WIDTH end as float)/1000
      + cast(case when BM6.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM6.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM6.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM6.KEIYAKU_SIZE_WIDTH end as float)/1000, 2) as 面積,
  round(
      (cast(case when BM1.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM1.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM1.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM1.KEIYAKU_SIZE_WIDTH end as float)/1000)/1.62
      + (cast(case when BM2.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM2.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM2.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM2.KEIYAKU_SIZE_WIDTH end as float)/1000)/1.62
      + (cast(case when BM3.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM3.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM3.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM3.KEIYAKU_SIZE_WIDTH end as float)/1000)/1.62
      + (cast(case when BM4.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM4.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM4.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM4.KEIYAKU_SIZE_WIDTH end as float)/1000)/1.62
      + (cast(case when BM5.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM5.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM5.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM5.KEIYAKU_SIZE_WIDTH end as float)/1000)/1.62
      + (cast(case when BM6.KEIYAKU_SIZE_HEIGHT is null then 0.0 else BM6.KEIYAKU_SIZE_HEIGHT end as float)/1000 * cast(case when BM6.KEIYAKU_SIZE_WIDTH is null then 0.0 else BM6.KEIYAKU_SIZE_WIDTH end as float)/1000)/1.62, 2) as 畳数,
  KA.KANBAN_BANSHITA as 板下,
  KA.KANBAN_BANSHITA + KA.KANBAN_SIZE_HEIGHT as GL,
  case when SI3.SELECT_ITEM_NM is null then '' else SI3.SELECT_ITEM_NM end as 柱径,
  case when SR.HASHIRA_HONSU is null then 0 else SR.HASHIRA_HONSU end as 柱本数,
  KS.SEKOU_YOTEI_YMD as 工事予定日,
  KS.CHAKOU_YOTEI_YMD as 着工予定日,
  SR.KIKAKU_SEKO_RECORD_NO as 工事回数,
  SR.SEKO_RECORD_YMD as 工事日,
  KS.SEKOU_KANRYO_YMD as 完了日,
  SRM.SEKO_MEMBER_CD as 施工メンバーCD,
  SS.SYAIN_NM as 施工メンバー名,
  SRM.LEADER_FLG as 施工リーダーフラグ,
  case when SRM.LEADER_FLG = 1 then 'リーダー' else '' end as リーダー,
  SR.SEKO_INFO as 施工指示
from
  SekouSiji.dbo.TKIKAKU as K
  left join SekouSiji.dbo.TKIKAKUKANBAN as KA on KA.KIKAKU_NO = K.KIKAKU_NO and KA.KIKAKU_KANBAN_NO = 1
  left join SekouSiji.dbo.MBAITAI as BA on BA.BAITAI_CD = K.BAITAI_CD
  left join SekouSiji.dbo.TKIKAKUSEKOU as KS on KS.KIKAKU_NO = K.KIKAKU_NO
  left join SekouSiji.dbo.M0002_SELECT_ITEM as SI on SI.SELECT_ITEM_CD = K.KIKAKU_KBN and SI.SI_PARENT_CD = 042
  left join SekouSiji.dbo.M0002_SELECT_ITEM as SI2 on SI2.SELECT_ITEM_CD = BA.BAITAI_SYUBETU_KBN and SI2.SI_PARENT_CD = 043
  left join SekouSiji.dbo.M0007_TODOHUKEN as TD on TD.TODOHUKEN_CD = K.TODOHUKEN_CD
  left join SekouSiji.dbo.MSYAIN as ES on ES.SYAIN_CD = K.EIGYO_TANTO_CD
  left join SekouSiji.dbo.TKIKAKUSEKORECORD as SR on SR.KIKAKU_NO = K.KIKAKU_NO
  left join SekouSiji.dbo.TKIKAKUSEKORECORDMEMBER as SRM on SRM.KIKAKU_NO = K.KIKAKU_NO and SRM.KIKAKU_SEKO_RECORD_NO = SR.KIKAKU_SEKO_RECORD_NO
  left join SekouSiji.dbo.M0002_SELECT_ITEM as SI3 on SI3.SELECT_ITEM_CD = SR.HASHIRA_SIZE and SI3.SI_PARENT_CD = 061
  left join SekouSiji.dbo.MSYAIN as SS on SS.SYAIN_CD = SRM.SEKO_MEMBER_CD
  left join SekouSiji.dbo.MBAITAIMEN as BM1 on (BM1.BAITAI_CD = BA.BAITAI_CD and BM1.MEN_CD = 01)
  left join SekouSiji.dbo.MBAITAIMEN as BM2 on (BM2.BAITAI_CD = BA.BAITAI_CD and BM2.MEN_CD = 02)
  left join SekouSiji.dbo.MBAITAIMEN as BM3 on (BM3.BAITAI_CD = BA.BAITAI_CD and BM3.MEN_CD = 03)
  left join SekouSiji.dbo.MBAITAIMEN as BM4 on (BM4.BAITAI_CD = BA.BAITAI_CD and BM4.MEN_CD = 04)
  left join SekouSiji.dbo.MBAITAIMEN as BM5 on (BM5.BAITAI_CD = BA.BAITAI_CD and BM5.MEN_CD = 05)
  left join SekouSiji.dbo.MBAITAIMEN as BM6 on (BM6.BAITAI_CD = BA.BAITAI_CD and BM6.MEN_CD = 06)
where
  K.KIKAKU_NO < 900000000
  and KS.SEKOU_YOTEI_YMD is not null
  and SR.KIKAKU_SEKO_RECORD_NO is not null
  and K.UPD_DATE >= DATEADD(hour, -1, DATEADD(hour, 9, GETDATE()))
order by
  K.KIKAKU_NO desc
  `;
  const rows = await executeQuery(sqlQuery);
  console.log('本日工事のSQL取得件数:', rows.length);
  return rows;
}

async function getTodayRecordsFromSql() {
  const sqlQuery = `
    SELECT
      K.KIKAKU_NO AS 企画番号,
      SR.KIKAKU_SEKO_RECORD_NO AS 工事回数
    FROM SekouSiji.dbo.TKIKAKU AS K
      LEFT JOIN SekouSiji.dbo.TKIKAKUSEKORECORD AS SR ON SR.KIKAKU_NO = K.KIKAKU_NO
      LEFT JOIN SekouSiji.dbo.TKIKAKUSEKOU AS KS ON KS.KIKAKU_NO = K.KIKAKU_NO
    WHERE
      K.KIKAKU_NO < 900000000
      AND KS.SEKOU_YOTEI_YMD IS NOT NULL
      AND SR.KIKAKU_SEKO_RECORD_NO IS NOT NULL
      AND TRY_CONVERT(date, SR.SEKO_RECORD_YMD) = CAST(DATEADD(HOUR, 9, GETDATE()) AS DATE)
  `;
  const rows = await executeQuery(sqlQuery);
  console.log('削除判定用の本日工事SQL取得件数:', rows.length);
  return rows;
}

const getMembersByKikakuNo = async (kikakuNo, sekoRecordNo) => {
    let kintoneUsers = [];
    try {
      const records = await SYAINClient.record.getAllRecords({
        app: 39, // 従業員マスタのアプリID
      });
      kintoneUsers = records.map(rec => ({
        userId: rec['ユーザーID'].value,
        syainNm: rec['社員名'].value,
        syainCd: rec['社員CD'].value,
      }));
    } catch (error) {
      console.error('Kintone API Error (従業員マスタ取得):', error);
      return []; // Kintone APIエラー時は空配列を返す
    }
    console.log(sekoRecordNo);

    let members = [];
    try {
        if (!dbPool) {
            dbPool = await createSqlConnection();
        }
        const sqlQuery = `
            SELECT SS.SYAIN_CD, SS.SYAIN_NM
            FROM dbo.TKIKAKUSEKORECORDMEMBER SRM
            LEFT JOIN dbo.TKIKAKUSEKORECORD SR
                ON SR.KIKAKU_NO = SRM.KIKAKU_NO 
                AND SR.KIKAKU_SEKO_RECORD_NO = SRM.KIKAKU_SEKO_RECORD_NO
            LEFT JOIN SekouSiji.dbo.MSYAIN SS
                ON SS.SYAIN_CD = SRM.SEKO_MEMBER_CD
            WHERE SRM.KIKAKU_NO = @kikakuNo
                AND SR.KIKAKU_SEKO_RECORD_NO = @sekoRecordNo
        `;

        const rows = await executeQuery(sqlQuery, [
            { name: 'kikakuNo', type: sql.VarChar, value: kikakuNo },
            { name: 'sekoRecordNo', type: sql.VarChar, value: String(sekoRecordNo) }
        ]);
        console.log('取得したメンバーの行:', rows);  // ここで結果を確認する
        members = rows.map(row => {
            const matchedUser = kintoneUsers.find(u => u.syainCd === row.SYAIN_CD);
            return { code: matchedUser ? matchedUser.userId : row.SYAIN_CD };
        });
        return members;

    } catch (error) {
        console.error('getMembersByKikakuNoでエラーが発生しました。', error);
        return [];
    }
};

/**
 * ③ kintoneから「工事日=今日」の全レコードのKEYを取得
 */
async function getTodayKeysFromKintone() {
  const todayStr = getTodayYYYYMMDD();
  const query = `工事日 = "${todayStr}"`; // 工事日フィールドが yyyy-MM-dd で記録されている前提
  const allRecords = await kintoneClient.record.getAllRecords({
    app: KINTONE_APP_ID,
    query,
    fields: ['KEY'],
  });
  console.log('本日工事のkintone取得件数:', allRecords.length);
  return allRecords.map(r => r.KEY.value);
}

/**
 * ④ UPSERT処理
 *   企画番号 + 工事回数 の複合キーで既存レコード検索→更新
 */
const upsertIntoKintone = async (record) => {
  const projectNo = record['企画番号'];
  const sekoRecordNo = record['工事回数']; // 工事回数として利用
  const compositeKey = `${projectNo}_${sekoRecordNo}`;
  console.log('型:', typeof sekoRecordNo, '値:', sekoRecordNo);
  console.log(`Processing 企画番号: ${projectNo}, 工事回数: ${sekoRecordNo}`);

  const members = await getMembersByKikakuNo(projectNo, sekoRecordNo);
  const detailData = record;

  try {
      const query = `企画No = "${projectNo}" and KIKAKU_SEKO_RECORD_NO = "${sekoRecordNo}"`;
      const getResponse = await kintoneClient.record.getRecords({
          app: KINTONE_APP_ID,
          query: query,
      });

      const kintoneRecord = mapRecordToKintoneFields(record, detailData ?? record);
      kintoneRecord['KEY'] = { value: compositeKey };
      kintoneRecord['メンバー'] = { value: members };
      kintoneRecord['人数'] = { value: members.length };

      if (getResponse.records && getResponse.records.length > 0) {
          const recordId = getResponse.records[0].$id.value;
          await kintoneClient.record.updateRecord({
              app: KINTONE_APP_ID,
              id: recordId,
              record: kintoneRecord,
          });
          console.log(`企画番号: ${projectNo} のレコードを更新しました。`);
      } else {
          await kintoneClient.record.addRecord({
              app: KINTONE_APP_ID,
              record: kintoneRecord,
          });
          console.log(`企画番号: ${projectNo} の新しいレコードを作成しました。`);
      }
  } catch (error) {
      console.error(`企画番号: ${projectNo} の処理中にエラーが発生しました。`, error);
      if (error.response) {
          console.error('ステータスコード:', error.response.status);
          console.error('エラーレスポンスデータ:', error.response.data);
      } else {
          console.error('エラーメッセージ:', error.message);
      }
  }
};

/**
 * ⑤ SQL→kintone マッピング
 */
const mapRecordToKintoneFields = (record, detailData) => {
  let latitude = null, longitude = null;
  if (record['緯度経度']) {
      const coords = String(record['緯度経度']).split(',');
      if (coords.length === 2) {
          latitude = parseFloat(coords[0]);
          longitude = parseFloat(coords[1]);
      }
  }

  const safeString = val => (val === undefined || val === null ? '' : String(val));
  const safeNumber = val => (val === undefined || val === null || val === '' ? null : (isNaN(Number(val)) ? null : Number(val)));
  const safeDate = val => {
      if (!val) return null;
      const d = new Date(val);
      return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
  };
  function extractNumber(str) {
      if (!str) return null;
      const m = str.match(/\d+/);
      return m ? Number(m[0]) : null;
  }

  const kintoneRecord = {
      企画No: { value: safeString(record['企画番号']) },
      設置場所: { value: safeString(detailData['設置場所']) },
      都道府県: { value: safeString(detailData['都道府県']) },
      市区群: { value: safeString(detailData['市区郡']) },
      面数: { value: safeString(detailData['媒体構成']) },
      着工予定日: { value: safeDate(detailData['着工予定日']) },
      工事予定日: { value: safeDate(detailData['工事予定日']) },
      工事日: { value: safeDate(detailData['工事日']) },
      完了日: { value: safeDate(detailData['完了日']) },
      営業名: { value: safeString(detailData['営業']) },
      企画区分: { value: safeString(detailData['区分']) },
      内容: { value: safeString(detailData['内容']) },
      緯度: { value: latitude !== null? safeNumber(latitude) : null },
      経度: { value: longitude !== null ? safeNumber(longitude) : null },
      板下: { value: detailData['板下'] ? safeNumber(detailData['板下']) : null },
      柱本数: { value: detailData['柱本数'] ? safeNumber(detailData['柱本数']) : null },
      柱サイズ: { value: detailData['柱径'] ? extractNumber(detailData['柱径']) : null },
      畳数: { value: detailData['畳数'] ? safeNumber(detailData['畳数']) : null },
      GL: { value: detailData['GL'] ? safeNumber(detailData['GL']) : null },
      KIKAKU_SEKO_RECORD_NO: { value: safeString(detailData['工事回数']) },
        };
      
        console.log('[mapRecordToKintoneFields] 送信予定データ:', JSON.stringify(kintoneRecord, null, 2));
        return kintoneRecord;
      }

// 日付を 'YYYY-MM-DD'に変換
function dateToString(val) {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().split('T')[0];
}

/**
 * ⑥ メイン処理
 *   1) reccnt>0 チェック (S_ReplicaDay)
 *   2) チェックOKなら当日レコードSQL取得→kintone UPSERT
 *   3) kintone上の当日KEY一覧を取得→SQLに存在しないものを削除
 */
async function main() {
  try {
    
    // (1) reccnt チェック
    const newDataExists = await hasReccntData();
    if (!newDataExists) {
      console.log('レプリケーション0件のため同期スキップします。');
      return;
    }

    // (2) SQLから本日の工事データ全件取得 → kintoneにUPSERT
    const sqlRecords = await getUpsertRecordsFromSql();
    const cloudKeys = [];
    for (const record of sqlRecords) {
      await upsertIntoKintone(record);
      const compositeKey = `${record['企画番号']}_${record['工事回数']}`;
      cloudKeys.push(compositeKey);
    }

    console.log('本日の工事データ同期完了しました。');

  } catch (error) {
    console.error('mainエラー:', error);
  } finally {
    if (dbPool) {
      try {
        await dbPool.close();
        console.log('DB接続をクローズしました');
      } catch (closeErr) {
        console.error('クローズ時エラー:', closeErr);
      }
    }
  }
}

/**
 * エンドポイント
 */
app.get('/', async (req, res) => {
  let localDbPool = null;
  try {
    localDbPool = await createSqlConnection();
    dbPool = localDbPool;
    await main();
    res.send('本日の工事データ同期が完了しました。');
  } catch (err) {
    console.error('エンドポイントエラー:', err);
    res.status(500).send('エラー:' + err.message);
  } finally {
    if (localDbPool) {
      try {
        await localDbPool.close();
      } catch (closeErr) {
        console.error('DBクローズ時エラー:', closeErr);
      }
    }
  }
});

// サーバ起動
app.listen(PORT, () => {
  console.log(`サーバー起動: ポート ${PORT}`);
});

