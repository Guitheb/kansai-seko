const express = require('express');
const sql = require('mssql');
const { Connector } = require('@google-cloud/cloud-sql-connector');
const { KintoneRestAPIClient } = require('@kintone/rest-api-client');
const cookieParser = require('cookie-parser');

const app = express();
const PORT = process.env.PORT || 8080;

// ミドルウェア設定
app.use(cookieParser());
app.use(express.urlencoded({ extended: true }));

// 認証ミドルウェア：Cookieにauthenticated=trueが設定されている場合は次に進む
function requireAuth(req, res, next) {
  if (req.cookies && req.cookies.authenticated === 'true') {
    return next();
  }
  res.redirect('/login');
}

const {
  DB_USER,
  DB_PASSWORD,
  INSTANCE_CONNECTION_NAME,
  DB_NAME,
  KINTONE_DOMAIN,
  KINTONE_APP_ID,
  KINTONE_API_TOKEN,
  SYAIN_API_TOKEN,
  APP_SECRET_PASS // パスフレーズ認証用の環境変数
} = process.env;

if (!DB_USER || !DB_PASSWORD || !INSTANCE_CONNECTION_NAME || !DB_NAME || !KINTONE_DOMAIN || !KINTONE_APP_ID || !KINTONE_API_TOKEN || !APP_SECRET_PASS) {
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

// グローバル変数としてDB接続プールを宣言
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
  config.server = '35.200.74.95'; // 実際のサーバIP（例：'35.200.74.95'）

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

/**
 * SQLクエリ実行用関数
 */
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
 * 日付を 'YYYY-MM-DD' に変換する関数
 */
function dateToString(val) {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().split('T')[0];
}

/**
 * 企画Noと工事回数からSQLレコードを取得する関数
 */
async function getRecordFromSql(projectNo, sekoRecordNo) {
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
      case when BM1.MEN_CD is not null then format(BM1.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM1.KEIYAKU_SIZE_WIDTH,'0')  else '' end
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
      K.KIKAKU_NO = '${projectNo}'
      and SR.KIKAKU_SEKO_RECORD_NO = '${sekoRecordNo}'
  `;
  const rows = await executeQuery(sqlQuery);
  return rows;
}

async function getRecordsByPeriod(fromDate, toDate) {
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
      case when BM1.MEN_CD is not null then format(BM1.KEIYAKU_SIZE_HEIGHT,'0') + 'x' + format(BM1.KEIYAKU_SIZE_WIDTH,'0')  else '' end
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
    and K.KIKAKU_NO > 100000
    and K.UPD_DATE BETWEEN @from AND @to
  `;
  return await executeQuery(sqlQuery, [
    { name: 'from', type: sql.Date, value: fromDate },
    { name: 'to', type: sql.Date, value: toDate }
  ]);
}

/**
 * 従業員マスタからユーザー情報を取得し、SQLのメンバー情報と突合する関数
 */
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
    return [];
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
    console.log('取得したメンバーの行:', rows);
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
 * KintoneへUPSERT処理を行う関数
 */
const upsertIntoKintone = async (record) => {
  const projectNo = record['企画番号'];
  const sekoRecordNo = record['工事回数']; // 工事回数として利用
  const compositeKey = `${projectNo}_${sekoRecordNo}`;
  console.log('型:', typeof sekoRecordNo, '値:', sekoRecordNo);
  console.log(`Processing 企画番号: ${projectNo}, 工事回数: ${sekoRecordNo}`);

  const members = await getMembersByKikakuNo(projectNo, sekoRecordNo);
  // SQLから取得したレコードはdetailDataとして扱います（ここではrecordと同一）
  const detailData = record;

  try {
    const query = `企画No = "${projectNo}" and KIKAKU_SEKO_RECORD_NO = "${sekoRecordNo}"`;
    const getResponse = await kintoneClient.record.getRecords({
      app: KINTONE_APP_ID,
      query: query,
    });

    const kintoneRecord = mapRecordToKintoneFields(record, detailData);
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

// 以下、ユーティリティ関数

function safeString(val) {
  return (val === undefined || val === null) ? '' : String(val);
}

function safeDate(val) {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
}

function safeNumber(val) {
  const num = Number(val);
  return isNaN(num) ? 0 : num;
}

function extractNumber(val) {
  if (!val) return null;
  const match = String(val).match(/[\d\.]+/);
  return match ? Number(match[0]) : null;
}

/**
 * SQLのレコードと同じ値を使ってKintoneのフィールドへマッピングする関数
 * ※detailDataはここではrecordと同一ですが、必要に応じて個別処理可能
 */
function mapRecordToKintoneFields(record, detailData) {
  let latitude = null, longitude = null;
  if (record['緯度経度']) {
    const coords = String(record['緯度経度']).split(',');
    if (coords.length === 2) {
      latitude = parseFloat(coords[0]);
      longitude = parseFloat(coords[1]);
    }
  }
  return {
    企画No: { value: safeString(record['企画番号']) },
    設置場所: { value: safeString(record['設置場所']) },
    都道府県: { value: safeString(record['都道府県']) },
    市区群: { value: safeString(record['市区郡']) },
    面数: { value: safeString(record['媒体構成']) },
    着工予定日: { value: safeDate(record['着工予定日']) },
    工事予定日: { value: safeDate(record['工事予定日']) },
    工事日: { value: safeDate(record['工事日']) },
    完了日: { value: safeDate(record['完了日']) },
    営業名: { value: safeString(record['営業']) },
    企画区分: { value: safeString(record['区分']) },
    内容: { value: safeString(record['内容']) },
    緯度: { value: latitude !== null ? safeNumber(latitude) : null },
    経度: { value: longitude !== null ? safeNumber(longitude) : null },
    板下: { value: record['板下'] ? safeNumber(record['板下']) : null },
    柱本数: { value: record['柱本数'] ? safeNumber(record['柱本数']) : null },
    柱サイズ: { value: record['柱径'] ? extractNumber(record['柱径']) : null },
    畳数: { value: record['畳数'] ? safeNumber(record['畳数']) : null },
    GL: { value: record['GL'] ? safeNumber(record['GL']) : null },
    KIKAKU_SEKO_RECORD_NO: { value: safeString(record['工事回数']) },
  };
}

// ログインページ（GET）
app.get('/login', (req, res) => {
  res.send(`
    <html>
      <head>
        <meta charset="utf-8">
        <title>ログイン</title>
      </head>
      <body>
        <h2>パスフレーズ認証</h2>
        <form method="POST" action="/login">
          <input type="password" name="passphrase" required placeholder="パスフレーズを入力">
          <button type="submit">ログイン</button>
        </form>
      </body>
    </html>
  `);
});

// ログイン処理（POST）
app.post('/login', (req, res) => {
  const pass = req.body.passphrase;
  if (pass === process.env.APP_SECRET_PASS) {
    res.cookie('authenticated', 'true', { httpOnly: true}); // 1時間有効
    return res.redirect('/');
  }
  res.send('パスフレーズが間違っています。<a href="/login">再入力</a>');
});

// メインフォーム表示（認証必須）
app.get('/', requireAuth, (req, res) => {
  res.send(`
    <html>
      <head>
        <meta charset="utf-8">
        <title>UPSERT パッチ処理</title>
      </head>
      <body>
        <h1>UPSERT パッチ処理</h1>
        <form action="/patch" method="GET" style="margin-bottom: 1em;">
          <label for="projectNo">企画No:</label>
          <input type="text" id="projectNo" name="projectNo" required><br><br>
          <label for="sekoRecordNo">工事回数:</label>
          <input type="text" id="sekoRecordNo" name="sekoRecordNo" required><br><br>
          <button type="submit">UPSERT 開始</button>
        </form>

        <h2>リカバリー</h2>
        <form action="/recover" method="GET">
          <label for="fromDate">開始日:</label>
          <input type="date" id="fromDate" name="fromDate" required>
          &nbsp;
          <label for="toDate">終了日:</label>
          <input type="date" id="toDate" name="toDate" required>
          <button type="submit">UPSERT 開始</button>
        </form>
        <h2>注意事項</h2>
        <ul>
          <li>UPSERT処理は、指定した企画Noと工事回数に対して行われます。</li>
          <li>工事回数は、SQLの工事回数とKintoneの工事回数が一致する必要があります。</li>
          <li>リカバリーはある期間の障害時間の</li>
          <li>名前は空白まで一致する必要があります。</li>
        </ul>
      </body>
    </html>
  `);
});

// パッチ処理エンドポイント（認証必須）
app.get('/patch', requireAuth, async (req, res) => {
  const projectNo = req.query.projectNo;
  const sekoRecordNo = req.query.sekoRecordNo;

  if (!projectNo || !sekoRecordNo) {
    return res.status(400).send("企画No と 工事回数 の両方を指定してください。");
  }

  try {
    // SQL接続確立
    const localDbPool = await createSqlConnection();
    dbPool = localDbPool;

    // SQLから該当レコード取得
    const records = await getRecordFromSql(projectNo, sekoRecordNo);
    if (records.length === 0) {
      return res.status(404).send("指定したレコードはSQL側に存在しません。");
    }

    // 複数件あれば最初の1件でUPSERT
    const record = records[0];
    await upsertIntoKintone(record);

    res.send(`企画No: ${projectNo}、工事回数: ${sekoRecordNo} のUPSERT処理が完了しました。`);
    res.clearCookie('authenticated'); // 処理後は認証をクリア
  } catch (err) {
    console.error('パッチ処理エラー:', err);
    res.status(500).send("エラーが発生しました。");
  } finally {
    if (dbPool) {
      try {
        await dbPool.close();
        console.log('DB接続をクローズしました');
      } catch (closeErr) {
        console.error('DBクローズ時エラー:', closeErr);
      }
      dbPool = null;
    }
  }
});
// 一括リカバリーエンドポイント（認証必須）
app.get('/recover', requireAuth, async (req, res) => {
  const { fromDate, toDate } = req.query;
  if (!fromDate || !toDate) {
    return res.status(400).send('開始日・終了日を指定してください。');
  }

  try {
    if (!dbPool) {
      dbPool = await createSqlConnection();
    }

    // 期間だけでレコード取得
    const records = await getRecordsByPeriod(fromDate, toDate);
    if (records.length === 0) {
      return res.status(404).send('指定した期間のレコードが見つかりませんでした。');
    }

    // 取得レコードを順次UPSERT
    for (const record of records) {
      await upsertIntoKintone(record);
    }

    res.send(`期間: ${fromDate} ～ ${toDate} の ${records.length} 件をリカバリーしました。`);
    res.clearCookie('authenticated'); // 処理後は認証をクリア
  } catch (err) {
    console.error('リカバリー処理エラー:', err);
    res.status(500).send('リカバリー中にエラーが発生しました。');
  }
});
// Cloud Run上でサーバ起動
app.listen(PORT, () => {
  console.log(`サーバー起動: ポート ${PORT}`);
});
/*
gcloud run deploy patch `
>>   --image gcr.io/sekonippou/patch:latest `
>>   --platform managed `
>>   --region asia-northeast2 `
>>   --allow-unauthenticated `
>>   --set-env-vars "DB_USER=fujiyagouser,DB_PASSWORD=fujiyagofujiyago,DB_NAME=SekouSiji,INSTANCE_CONNECTION_NAME=sekonippou:asia-northeast1:seko-sandbox,KINTONE_DOMAIN=kansai-seko,KINTONE_APP_ID=34,KINTONE_API_TOKEN=fFe4LVZFm8BGcZEjcjYZDgvvkCmd6uouUz4goR4T,SYAIN_API_TOKEN=HhOJpvxLb7iSPviU3A4iFlbidEbiW7D1E6ElhYw2,APP_SECRET_PASS=kansai-seko"*/