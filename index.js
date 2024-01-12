const { MasterTableConfig, TableValidation, RecentlyAccessedTable, DCUStatsTableData } = require("../../models");
const { makeConnection } = require("../../db/db");
const knex = require("../../db/knex");
const qs = require("qs");
const winston = require("../../config/logger")("controller/crudController/index.js");
const { callSproc } = require("../../helpers/storedProcedure");
const { getDateFormat } = require("../../helpers");
const moment = require("moment");
const { AsyncParser } = require("json2csv");
const csvParse = require("csv-parse");

const { generateUserTables } = require("../userController");

const {
  AUTO_TIMESTAMP,
  AUTO_USER,
  CRUD_OPERATION_COL_NAME,
  CRUD_DELETE,
  CRUD_INSERT,
  CRUD_UPDATE,
  CRUD_UPSERT,
  COL_DATE_TYPE,
  COL_INTEGER_TYPE,
  COL_DECIMAL_TYPE,
  COL_BOOLEAN_TYPE,
  COL_STRING_TYPE,
  VALIDATION_TYPE,
  MOBIUS_APP_NAME,
  TIMEOUT,
  COL_JSON_TYPE,
  COL_ARRAY_TYPE,
  DMY_FORMAT_STRING,
  YMDHMS_FORMAT_STRING,
  YMD_FORMAT_STRING,
  HHMM_FORMAT_STRING,
  HHMMSS_FORMAT_STRING,
} = require("../../helpers/constants");

const { transformDate_bulkUpload, transformDate_uiInsert, getAppDateFormat } = require("./transformDate");
const { buildFilter, buildFilterWithEntitlement, buildQueryWithEntityControl, checkUserEntitlementApps } = require("./build");
const e = require("cors");

// check user permissions for the table - throws errors
async function checkUserPermissions(req, app, table) {
  const userTables = await generateUserTables(req);
  const userTable = userTables.find((_table) => _table.table_name === table && _table.application_name === app);
  if (!userTable) {
    throw new Error("your are not entitled to the app");
  } else {
    if (userTable.isModify !== "true") {
      throw new Error("your are not entitled to this operation"); // isRead
    }
  }
}

function parseStatsResults(result, dbType) {
  let resultTemp;
  if (typeof dbType == "function") {
    resultTemp = result.rows.length > 0 ? result.rows[0] : 0;
    return resultTemp ? resultTemp.row_cnt : 0;
  } else if (dbType === "mssql") {
    resultTemp = result;
    return resultTemp ? resultTemp[0].row_cnt : 0;
  } else {
    resultTemp = result;
    return resultTemp ? resultTemp[0].ROW_CNT : 0;
  }
}

// first api
exports.getTableDataAndCount = async (req, res, next) => {
  let db = "";
  const app = req.params.app;
  try {
    const { nameID } = req.user;
    db = await makeConnection(app, nameID);
    const tableName = req.params.table;
    const groups = req.user.group;
    let page = parseInt(req.query.page) || 1; //optional
    const limit = parseInt(req.query.limit) || 20; //optional
    const column = req.query.sortBy;
    const order = req.query.orderBy || "asc"; //optional
    let filters = qs.parse(req.query.filters || []); //optional
    let collectStatsFlag = req.query.stats || "N"; //optional

    const config = await MasterTableConfig.find((qb) => {
      qb.where({ table_name: tableName }).andWhere({
        application_name: req.params.app,
      });
    });

    for (let key in filters) {
      const filter = filters[key];
      const colConfig = config.find((col) => {
        return col.data_type === COL_DATE_TYPE && filter.value && filter.name === col.column_name;
      });
      if (colConfig) {
        filter.value = transformDate_uiInsert(filter.value, colConfig.format, db);
      }
    }

    let query = req.query.select_query;
    let queryWhere = req.query.select_query_where;
    let queryOrder = req.query.select_query_order;
    let selectCountQuery = `SELECT COUNT(1) as row_cnt FROM ${tableName}`;
    let selectCountFullQuery = selectCountQuery;
    let selectCountFullWithWhereQuery = selectCountQuery;

    if (!query) return res.status(422).send("query not found");

    if (queryWhere) {
      const builtWhereQuery = await buildQueryWithEntityControl(app, tableName, groups, queryWhere);
      query += ` ${builtWhereQuery}`;
      selectCountQuery += ` ${builtWhereQuery}`;
      selectCountFullWithWhereQuery += ` ${builtWhereQuery}`;

      query = buildFilterWithEntitlement(filters, query, tableName, typeof db.client.config.client === "function");
      selectCountQuery = buildFilterWithEntitlement(filters, selectCountQuery, tableName, typeof db.client.config.client === "function");
    } else {
      query = buildFilter(filters, query, tableName, typeof db.client.config.client === "function");
      selectCountQuery = buildFilter(filters, selectCountQuery, tableName, typeof db.client.config.client === "function");
    }

    // query += queryOrder
    //   ? ` ${queryOrder}`
    //   : column
    //   ? ` order by ${column} ${order}`
    //   : "";

    query += queryOrder ? ` ${queryOrder}  offset ${(page - 1) * limit} row fetch next ${limit} rows only` : `${column ? ` order by ${column} ${order}` : ""} offset ${(page - 1) * limit} row fetch next ${limit} rows only`;
    console.log('query ', query)
    const recentTableQuery = RecentlyAccessedTable.create({
      ntid: nameID,
      application_name: req.params.app,
      table_name: tableName,
      viewed_at: new Date(),
    }).toString();

    const result = await Promise.all([db.raw(query), knex.raw(recentTableQuery), db.raw(selectCountQuery)]);

    const rows = typeof db.client.config.client === "string" ? result[0] : result[0].rows;

    const transformResult = rows.map((row) => {
      let newRow = {};
      for (let key in row) {
        const col = config.find((_col) => _col.column_name.toLowerCase() === key.toLowerCase());

        if (typeof row[key] !== "string" && col.form_editor_ui === "time" && row[key]) {
          //console.log("RETRIEVALGETTABLEDATA!!!!!!!!!!!!!!!!", "col", col.column_name, "val", row[key], "datatype", col.data_type, "valuetype", typeof row[key])
          if (db.client.config.client === "mssql") {
            newRow[key] = moment.utc(row[key]).format(col.format);
          } else {
            newRow[key] = moment(row[key]).format(col.format);
          }
        }

        if (col && col.data_type.toLowerCase() === "datetime" && row[key] && db.client.config.client !== "mssql" && typeof db.client.config.client !== "function") {
          const rawTime = new Date(row[key].toString().replace(/"/g, "")); // + 8 hrs
          newRow[key] = new Date(rawTime.getTime() + 28800000);
        }
      }
      return { ...row, ...newRow };
    });

    //Attempt to handle Snowflake VS RDBMS count result return
    const resultFilterCount = parseStatsResults(result[2], db.client.config.client);

    if (collectStatsFlag === "Y") {
      let findAppWithEntitlementQuery = "SELECT DISTINCT APPLICATION_NAME as app_name from TBLMOB_CFG_DCU_ENT_CONTROL";

      const findAppWithEntitlementData = await Promise.all([knex.raw(findAppWithEntitlementQuery)]);

      let appWithEntitlementData = findAppWithEntitlementData[0];
      const appWithEntitlement = appWithEntitlementData.map((row) => {
        return row.app_name;
      });

      let isAppWithEntitlement = appWithEntitlement.includes(req.params.app);

      const statsKeyData = {
        application_name: req.params.app,
        table_name: tableName,
      };

      const findStatsDataQuery = DCUStatsTableData.find(statsKeyData).toString();

      console.log(
        "Application with entitlement: ",
        isAppWithEntitlement,
        "Query to execute for count:",
        isAppWithEntitlement ? selectCountFullQuery : selectCountFullWithWhereQuery,
      );

      const findStatsData = await Promise.all([knex.raw(findStatsDataQuery), isAppWithEntitlement ? db.raw(selectCountFullQuery) : db.raw(selectCountFullWithWhereQuery)]);

      const resultFullCount = parseStatsResults(findStatsData[1], db.client.config.client);

      const statsData = {
        application_name: req.params.app,
        table_name: tableName,
        num_of_rec: resultFullCount,
        updated_at: new Date(),
        updated_by: nameID,
      };

      const collectStatsQuery = findStatsData[0].length > 0 ? DCUStatsTableData.updateByKey(statsData, statsKeyData).toString() : DCUStatsTableData.create(statsData).toString();

      await Promise.all([knex.raw(collectStatsQuery)]);
    }

    return res.status(200).json({
      count: resultFilterCount,
      data: transformResult,
    });
  } catch (e) {
    winston.error(req, e);
    res.status(500).json({ message: !db ? `Error connecting to ${app}` : e.message });
    return next(e);
  } finally {
    if (db) db.destroy();
  }
};

exports.getTableData = async (req, res, next) => {
  let db = "";
  const app = req.params.app;
  try {
    db = await makeConnection(app, req.user.nameID);
    const tableName = req.params.table;
    const groups = req.user.group;
    let page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const column = req.query.sortBy;
    const order = req.query.orderBy || "asc";
    let filters = qs.parse(req.query.filters || []);
    const config = await MasterTableConfig.find((qb) => {
      qb.where({ table_name: tableName }).andWhere({
        application_name: req.params.app,
      });
    });

    for (let key in filters) {
      const filter = filters[key];
      const colConfig = config.find((col) => {
        return col.data_type === COL_DATE_TYPE && filter.value && filter.name === col.column_name;
      });
      if (colConfig) {
        filter.value = transformDate_uiInsert(filter.value, colConfig.format, db);
      }
    }
    let query = req.query.select_query;
    let queryWhere = req.query.select_query_where;
    let queryOrder = req.query.select_query_order;
    if (!query) return res.status(422).send("query not found");
    winston.info(req, `page: ${page}, limit: ${limit}, sort: ${order}, column: ${column}, filters: ${filters}`);
    if (page < 1) page = 1;
    if (queryWhere) {
      const builtWhereQuery = await buildQueryWithEntityControl(app, tableName, groups, queryWhere);
      query += ` ${builtWhereQuery}`;
      query = buildFilterWithEntitlement(filters, query, tableName, typeof db.client.config.client === "function");
    } else {
      query = buildFilter(filters, query, tableName, typeof db.client.config.client === "function");
    }

    query += queryOrder ? ` ${queryOrder} offset ${(page - 1) * limit} row fetch next ${limit} rows only` : `${column ? ` order by ${column} ${order}` : ""} offset ${(page - 1) * limit} row fetch next ${limit} rows only`;
    console.log(query)
    const rawData = await db.raw(query);
    const rows = typeof db.client.config.client === "string" ? rawData : rawData.rows;
    const data = rows.map((row) => {
      let newRow = {};
      for (let key in row) {
        const col = config.find((col) => col.column_name.toLowerCase() === key.toLowerCase());

        if (typeof row[key] !== "string" && col.form_editor_ui === "time" && row[key]) {
          //console.log("RETRIEVALGETTABLEDATA!!!!!!!!!!!!!!!! GTD", "col", col.column_name, "val", row[key], "datatype", col.data_type, "valuetype", typeof row[key])
          if (db.client.config.client === "mssql") {
            newRow[key] = moment.utc(row[key]).format(col.format);
          } else {
            newRow[key] = moment(row[key]).format(col.format);
          }
          //console.log("valueafter", newRow[key])
        }

        if (col && col.data_type.toLowerCase() === "datetime" && row[key] && db.client.config.client !== "mssql" && typeof db.client.config.client !== "function") {
          const aa = new Date(row[key].toString().replace(/"/g, "")); // + 8 hrs
          newRow[key] = new Date(aa.getTime() + 28800000);
        }
      }
      return { ...row, ...newRow };
    });

    return res.status(200).json({ data });
  } catch (e) {
    winston.error(req, e);
    res.status(500).json({
      message: !db ? `Error connecting to ${app}` : e.message,
    });
    return next(e);
  } finally {
    if (db) db.destroy();
  }
};

exports.deleteTableRow = async (req, res, next) => {
  let db = "";
  try {
    db = await makeConnection(req.params.app, req.user.nameID);
    const { table } = req.params;
    await checkUserPermissions(req, req.params.app, table);
    const cols = req.body;
    const keys = Object.keys(cols);
    if (!cols || keys.length === 0) res.status(403).json({ message: "no column selected" });
    const config = await MasterTableConfig.find((qb) => {
      qb.where({ table_name: table }).andWhere({
        application_name: req.params.app,
      });
    });

    if (typeof db.client.config.client === "function") {
      var key_main,
        key_sub = Object.keys(req.body);
      var n = key_sub.length;
      var cols_lowercase = {};
      while (n--) {
        key_main = key_sub[n];
        cols_lowercase[key_main.toLowerCase()] = req.body[key_main];
      }
      const keys_lowercase = Object.keys(cols_lowercase);

      let totalNoOfrecords = 0;
      let output = [];
      config.map((col) => {
        if (cols_lowercase[col.column_name.toLowerCase()]) {
          totalNoOfrecords = cols_lowercase[col.column_name.toLowerCase()].length;
        }
      });
      var i;
      for (i = 0; i < totalNoOfrecords; i++) {
        var temp = {};
        config.map((col) => {
          if (cols_lowercase[col.column_name.toLowerCase()]) {
            temp[col.column_name.toLowerCase()] = cols_lowercase[col.column_name.toLowerCase()][i];
          }
        });
        output.push(temp);
      }
      try {
        for (let row of output) {
          winston.info(req, req.user.nameID);
          await db(table)
            .del(row)
            .where((qb) => {
              keys_lowercase.map((identifier, idx) => {
                if (idx === 0) qb.where(identifier.toLowerCase(), row[identifier.toLowerCase()]);
                else {
                  qb.andWhere(identifier.toLowerCase(), row[identifier.toLowerCase()]);
                }
              });
            });
        }
      } catch (e) {
        // errors.push({ line, msg: e.message });
        winston.error(req, e);
        return res.status(406).json({ message: e.message });
      }
    } else {
      let totalNoOfrecords = 0;
      let output = [];
      config.map((col) => {
        if (cols[col.column_name]) {
          totalNoOfrecords = cols[col.column_name].length;
        }
      });
      var i;
      for (i = 0; i < totalNoOfrecords; i++) {
        var temp = {};
        config.map((col) => {
          if (cols[col.column_name]) {
            temp[col.column_name] = cols[col.column_name][i];
          }
        });
        output.push(temp);
      }
      try {
        for (let row of output) {
          winston.info(req, req.user.nameID);
          await db(table)
            .del()
            .where((qb) => {
              config.map((col) => {
                if (col.data_type === COL_DATE_TYPE && row[col.column_name]) {
                  console.log("123123123123123123", db.client.config.client)
                  //Sepcial Handling to convert the Dates to the specific formats before sending it to the Database
                  if (db.client.config.client === "mssql" || db.client.config.client === "oracledb" )  {
                    row[col.column_name] = transformDate_uiInsert(row[col.column_name], col.format, db);
                  } else {
                    row[col.column_name] = new Date(row[col.column_name]);
                  }
                }
              });
              keys.map((identifier, idx) => {
                if (idx === 0) qb.where(identifier, row[identifier]);
                else {
                  qb.andWhere(identifier, row[identifier]);
                }
              });
            });
        }
      } catch (e) {
        // errors.push({ line, msg: e.message, });
        return res.status(406).json({ message: "no matching record found" });
      }
    }
    return res.status(200).json({ message: "success", data: "Deleted" });
  } catch (e) {
    winston.error(req, e);
    res.status(500).json({
      message: !db ? `Error connecting to ${req.params.app}` : e.message,
    });
    return next(e);
  } finally {
    if (db) db.destroy();
  }
};

exports.deleteAllTableData = async (req, res, next) => {
  let db = "";
  try {
    db = await makeConnection(req.params.app, req.user.nameID);
    const { table } = req.params;
    await checkUserPermissions(req, req.params.app, table);
    winston.info(req, req.user.nameID);
    await db(table).del();
    return res.status(200).json({ message: "success" });
  } catch (e) {
    winston.error(req, e);
    res.status(500).json({
      message: !db ? `Error connecting to ${req.params.app}` : e.message,
    });
    return next(e);
  } finally {
    if (db) db.destroy();
  }
};

exports.exportData = async (req, res, next) => {
  let db = "";
  
  const transformOpts = { highWaterMark: 8192 };
  try {
    winston.info(req, req.user.nameID);
    db = await makeConnection(req.params.app, req.user.nameID);
    const tableName = req.params.table;
    const column = req.query.sortBy;
    const order = req.query.orderBy || "asc";
    const config = await MasterTableConfig.find((qb) => {
      qb.where({ table_name: tableName }).andWhere({
        application_name: req.params.app,
      });
    });

    let filters = qs.parse(req.query.filters || []);
    let query = req.query.select_query;
    let queryWhere = req.query.select_query_where;
    const groups = req.user.group;
    if (!query) return res.status(422).send("query not found");
    winston.info(req, `sort: ${order}, column: ${column}`);

    for (let key in filters) {
      const filter = filters[key];
      const colConfig = config.find((col) => {
        return col.data_type === COL_DATE_TYPE && filter.value && filter.name === col.column_name;
      });
      if (colConfig) {
        filter.value = transformDate_uiInsert(filter.value, colConfig.format, db);
      }
    }

    if (queryWhere) {
      const builtWhereQuery = await buildQueryWithEntityControl(req.params.app, tableName, groups, queryWhere);
      query += ` ${builtWhereQuery}`;
      query = buildFilterWithEntitlement(filters, query, tableName, typeof db.client.config.client === "function");
    } else {
      query = buildFilter(filters, query, tableName, typeof db.client.config.client === "function");
    }

    //query = buildFilter(filters, query, tableName,  typeof db.client.config.client === "function");
    query += column ? ` order by ${column} ${order}` : "";
    const result = await db.raw(query);
    
    const rows = typeof db.client.config.client === "string" ? result : result.rows;
    if (!rows.length) return res.status(200).json({ data: rows, message: "No rows found in database" });
    const transformResult = rows.map((row) => {
      let newRow = {};
      for (let key in row) {
        const col = config.find((col) => col.column_name.toLowerCase() === key.toLowerCase());
        if (
          col &&
          (col.data_type.toLowerCase() === "datetime" || col.bind_value.toLowerCase() === "systemtime" || col.form_editor_ui === "time") &&
          row[key] &&
          row[key] !== "NULL"
        ) {
                    if (typeof row[key] === "string") {
            if (col.form_editor_ui === "time") {
              newRow[key] = moment(row[key], col.format).format(col.format);
            } else {
              if (db.client.config.client === "mssql") {
                newRow[key] = moment.utc(row[key]).format(col.format);
              } else {
                newRow[key] = moment(row[key]).format(col.format);
              }
            }
          } else if (typeof row[key] !== "string") {
            if (col.form_editor_ui === "time") {
              //console.log("!!!!!!!!!", "col:", col.column_name, "BEFOREVAL", row[key], "key", key, "type", typeof row[key])
              newRow[key] = moment.utc(row[key]).format(col.format);
            } 
            else {
              if (db.client.config.client === "mssql") {
                newRow[key] = moment.utc(row[key]).format(col.format);
              } else {
                newRow[key] = moment(row[key].toJSON()).format(col.format);
              }
            }
            if (col.data_type.toLowerCase()=="datetime" && db.client.config.client === "oracledb" )  {
                const rawTime = new Date(row[key].toString().replace(/"/g, "")); // + 8 hrs
                newRow[key] = transformDate_uiInsert(new Date(rawTime.getTime() + 28800000), col.format, db);
                // winston.info(req, `Before: ${row[key]}`);
                // row[key] = transformDate_uiInsert(row[key], col.format, db);
                // winston.info(req, `After: ${row[key]}`);
              }
          }
          // Logic to handle Date
          // winston.info(req, `Before: ${row[key]}`);
          // 
          // newRow[key] = moment(row[key]).format(
          //   col.format || "DD-MM-YYYY HH:mm:ss"
          // );
        }
      }
      return { ...row, ...newRow };
    });
    const fields = Object.keys(rows[0]);

    /* Removing the Async Parser as this is useless 
    const asyncParser = new AsyncParser({ fields }, transformOpts);
    let csv = "";
    asyncParser.processor
      .on("data", (chunk) => (csv += chunk.toString()))
      .on("end", () => res.status(200).json({ data: csv }))
      .on("error", (err) =>
        res.status(422).json({ error: err, message: "failure" })
      );

    asyncParser.input.push(JSON.stringify(transformResult)); // This data might come from an HTTP request, etc.
    asyncParser.input.push(null);
    */
    winston.info(req, `${JSON.stringify(transformResult)}`);
    return res.status(200).json(transformResult);
  } catch (e) {
    winston.error(req, e);
    res.status(500).json({
      message: !db ? `Error connecting to ${req.params.app}` : e.message,
    });
    return next(e);
  } finally {
    if (db) db.destroy();
  }
};

exports.getSearchData = async (req, res, next) => {
  try {
    const app = req.params.app;
    const dataFields = JSON.parse(req.query.fields);
    const page = req.query.page ? parseInt(req.query.page) : 0;
    const limit = req.query.limit ? parseInt(req.query.limit) : 0;
    const searchStr = req.query.search;
    winston.info(req, dataFields);
    winston.info(req, req.user.nameID);
    let result = await Promise.all(dataFields.map((field) => callSproc(app, field, page, limit, searchStr, req.user.nameID)));
    if (!result || result.length === 0) return (result = []);
    const formattedData = dataFields.reduce((accumultor, key, index) => {
      accumultor[key.column] = result[index];
      return accumultor;
    }, {});
    return res.status(200).json({ data: formattedData });
  } catch (e) {
    winston.error(req, e);
    return next(e);
  }
};

const doValidation = (validationConfig, record, linex = -1) => {
  //console.log("start doValidate", record, "appformat", getAppDateFormat())
  let validationError = [];
  let anotherRec = Object.assign({}, record);
  anotherRec = Object.keys(anotherRec).reduce(
    (prev, current) => ({
      ...prev,
      [current.toLowerCase()]: anotherRec[current],
    }),
    {},
  );

  if (validationConfig.length) {
    validationConfig.map((conf) => {
      const column = conf.control_to_validate.toLowerCase();
      const validationAgainstCol = conf.control_to_validate_against.toLowerCase();
      const valueToValidate = conf.value_to_validate;
      if (conf.validator_type.toLowerCase() === VALIDATION_TYPE.MAX_LENGTH && valueToValidate) {
        if (!anotherRec[column]) {
          //return validationError.push({ line: linex, msg: `Required column ${column} is empty`, });
        } else {
          if (anotherRec[column].length <= conf.value_to_validate) return;
          //console.log("doValidation_check_length", record, "appformat", getAppDateFormat())
          return validationError.push({
            line: linex,
            msg: `${conf.error_message}`,
          });
        }
      } else {
        const start = anotherRec[column] ? moment(anotherRec[column], getAppDateFormat()) : undefined;
        const end = anotherRec[validationAgainstCol] ? moment(anotherRec[validationAgainstCol], getAppDateFormat()) : undefined;
        // console.log('VVVVVVVVVVVVVVVVVVVV', conf.validator_type, column, validationAgainstCol, start, end, anotherRec)
        if (start) {
          if (conf.validator_type.toLowerCase() === VALIDATION_TYPE.DURATION_CHECK && end) {
            const duration = end.diff(start, "days");
            if (duration <= conf.value_to_validate) return;
            //console.log("doValidation_check_diff", record, "appformat", getAppDateFormat(), "duration", duration)
            return validationError.push({
              line: linex,
              msg: `${conf.error_message}`,
            });
          }
          if (conf.validator_type.toLowerCase() === VALIDATION_TYPE.DATE_COPARISON_CHECK && end) {
            if (moment(end).diff(moment(start), "days") > 0) return;
            //console.log("doValidation_compare", record, "appformat", getAppDateFormat(), "end", end, "start", start)
            return validationError.push({
              line: linex,
              msg: `${conf.error_message}`,
            });
          }
          const current = new Date(start).getTime();
          const validator = valueToValidate ? new Date(moment(valueToValidate, "DD/MM/YYYY", "Asia/Singapore")).getTime() : undefined;
          if (conf.validator_type.toLowerCase() === VALIDATION_TYPE.MINIMUM_DATE_CHECK && validator) {
            if (current >= validator) return;
            //console.log("doValidation_min", record, "appformat", getAppDateFormat(), "current", current, "validator", validator)
            return validationError.push({
              line: linex,
              msg: `${conf.error_message}`,
            });
          }
          if (conf.validator_type.toLowerCase() === VALIDATION_TYPE.MAXIMUM_DATE_CHECK && validator) {
            if (current <= validator) return;
            //console.log("doValidation_max", record, "appformat", getAppDateFormat(), "current", current, "validator", validator)
            return validationError.push({
              line: linex,
              msg: `${conf.error_message}`,
            });
          }
        }
      }
    });
  }
  //console.log("end doValidate", record, "appformat", getAppDateFormat())
  return validationError;
};

// To be set reusable for both Bulk and UI
const doColumnValidation = (col, col_value, db) => {
  if (col.data_type === COL_DATE_TYPE && col_value) {
    return transformDate_uiInsert(col_value, col.format, db);
  } else if (col.data_type === COL_BOOLEAN_TYPE) {
    if (col_value == undefined) return null;
    if (col_value === false) return "0"; //Currently ALL DB allows "0", "1". So we default FALSE to "0". If in the event there is a DB error due to this, we may have to handle it by db.
  } else if (col.data_type === COL_INTEGER_TYPE || col.data_type === COL_DECIMAL_TYPE) {
    if (col.form_editor_ui === "switch") {
      if (col_value == undefined) return null;
    } else if (col_value === 0 ) return "0" 
    else if (col_value == '' ) return null
    else if(!col_value) return null;
  } else if (col.data_type === "string" || col.form_editor_ui === "time") {
    if (!col_value) return null;
  }
  return col_value;
};

exports.updateTable = async (req, res) => {
  /*
    Pre-requisite for update table:
    Query String : cols : contains primary key
    Payload : body : contains all columns that SHOULD be updated. The check for editable / insertable should be done in frontend
  */
  let db = "";
  try {
    const app = req.params.app;
    db = await makeConnection(app, req.user.nameID);
    const { table } = req.params;
    await checkUserPermissions(req, app, table);

    const config = await MasterTableConfig.find({
      table_name: table,
      application_name: app,
    });
    const cols = req.query;
    const { body } = req;
    const keys = Object.keys(body);
    //const reference are immutable, but const referenced values are. We can still do this assignment
    //this is to update column key to all be upper case (due to Oracle requirement for Column Name to be upper cased)
    const bodyKeys = Object.keys(body);
    const colsKeys = Object.keys(cols);
    
    console.log("req.query : " , req.query)
    console.log("req.body : " , req.body)
    console.log("keys : " , keys);

    colsKeys.map((key) => {
      if (key.toUpperCase().trim() !== key) {
        cols[key.toUpperCase().trim()] = cols[key];
        delete cols[key];
      }
    });
    
    // const capBodyKeys = Object.keys(body)
    // if (typeof db.client.config.client === "function") {
    //   capBodyKeys.map((key) => {
    //    // if (col.column_name === key && col.data_type === "string") {
    //    //   body[key] = col.form_editor_ui === "time" ? "'" + moment(body[key], col.format).format(timeFormat) + "'" : "'" + body[key].replace(/'/g, "''") + "'";
    //     //} else if (col.column_name === key && col.data_type === COL_DATE_TYPE) {
    //       // Handling -> Included quotes for snowflake before insert
    //       console.log("body[key] :  " , body[key])
    //     //}
    //   });
    // }

    bodyKeys.map((key) => {
      if (key.toUpperCase().trim() !== key) {
        body[key.toUpperCase().trim()] = body[key];
        delete body[key];
      }
    });

    if (app.toLowerCase().includes(MOBIUS_APP_NAME)) {
      let appKey = "";
      if (Object.prototype.hasOwnProperty.call(body, "APPLICATION")) {
        appKey = "APPLICATION";
      }
      if (Object.prototype.hasOwnProperty.call(body, "APPLICATION_NAME")) {
        appKey = "APPLICATION_NAME";
      }
      if (appKey) {
        const userApps = await checkUserEntitlementApps(app, table, req.user.group);
        const appEntitled = userApps.find((userApp) => userApp.application_name === body[appKey]);
        if (!appEntitled) {
          throw new Error(`E0009 - This operation is prohibited becuase you are not entitled to this Application : ${body[appKey]}`);
        }
      }
    }


    console.log("Before Validation")
    //console.log(config)
    //console.log(cols);
    config.map((col) => {
      let column_name = col.column_name.toUpperCase().trim();

      const kk = Object.keys(body)
      //if (typeof db.client.config.client === "function") {
      //   kk.map((key) => {
      //    // if (col.column_name === key && col.data_type === "string") {
      //    //   body[key] = col.form_editor_ui === "time" ? "'" + moment(body[key], col.format).format(timeFormat) + "'" : "'" + body[key].replace(/'/g, "''") + "'";
      //     //} else if (col.column_name === key && col.data_type === COL_DATE_TYPE) {
      //       // Handling -> Included quotes for snowflake before insert
      //       console.log("body[key] :  " , body[key])
      //     //}
      //   });
      // //}

      if (cols[column_name] !== undefined) {
        cols[column_name] = doColumnValidation(col, cols[column_name], db);
      }

      if (body[column_name] !== undefined) {
        body[column_name] = doColumnValidation(col, body[column_name], db);
      }

      if (col.bind_value && col.bind_value.toLowerCase() === AUTO_USER)
        req.body[col.column_name] = req.user.nameID;
      if (col.bind_value && col.bind_value.toLowerCase() === AUTO_TIMESTAMP) {
        if (typeof db.client.config.client === "function") {
          if (col.format.includes(" "))
          {
            const aa = new Date(); // + 8 hrs
            req.body[col.column_name] =  moment.utc(new Date(aa.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING);
          }
          else
          {
            const aa = new Date(); // + 8 hrs
            req.body[column_name] = moment(new Date(), col.format).format(YMD_FORMAT_STRING);
            console.log("COMING HERE Ganesh Kumar \n\n\n\n")
            console.log(new Date(aa.getTime() + 28800000))
            console.log(moment(new Date(), col.format).format(YMD_FORMAT_STRING))
          }
          // if db is snowflake
        } else if (col.format.includes(" ")) {
          if (db.client.config.client === "mssql") {
            const aa = new Date(); // + 8 hrs
            req.body[col.column_name] = moment.utc(new Date(aa.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING);
          } else req.body[col.column_name] = new Date();
        } else {
          if (db.client.config.client === "mssql") {
            const aa = new Date(); // + 8 hrs
            req.body[col.column_name] = moment(moment.utc(new Date(aa.getTime() + 28800000)), col.format).format(YMDHMS_FORMAT_STRING);
          } else req.body[col.column_name] = moment(new Date(), col.format).format(DMY_FORMAT_STRING);
        }
      }

      if (typeof db.client.config.client === "function") {
        const timeFormat = col.format.toLowerCase().includes("ss") ? HHMMSS_FORMAT_STRING : HHMM_FORMAT_STRING;
        keys.map((key) => {
          console.log(body[key])
          if (col.column_name === key && col.data_type === "string") {
            body[key] = col.form_editor_ui === "time" ? "'" + moment(body[key], col.format).format(timeFormat) + "'" : "'" + body[key].replace(/'/g, "''") + "'";
          } else if (col.column_name === key && col.data_type === COL_DATE_TYPE) {
            // Handling -> Included quotes for snowflake before insert
            body[key] = "'" + body[key] + "'";
          }
        });
      }
    });

    // validation here
    const validationConfig = await TableValidation.find({
      table_name: table,
      application_name: app,
    });
    const errors = doValidation(validationConfig, body);
    if (errors.length) throw new Error(JSON.stringify(errors));


    //console.log("*********body*********")
    //console.log(body);
    //console.log(cols);
    
    /* 
    UPDATE TABLENAME SET = WHERE = ''

    `update INTO ${table} (${insertKeys.join(", ")}) select ${.join(", ")}`



    */

   let count = 0
   if (typeof db.client.config.client === "function") { 
    
   const updateColNames = Object.keys(body);
   const updateColValues = Object.values(body);
   const updatePkColName = Object.keys(cols);
   const updatePkColValues = Object.values(cols);

  //  console.log("After all the transformations ... ")
  //  updatePkColName.map((key) => {
  //    console.log(key)
  //  });
  //  console.log("After all the transformations ... ")
  //  updatePkColValues.map((key) => {
  //    console.log(key)
  //  });
   console.log("CONFIGS. ... ")
   
   for (let i=0; i<config.length; i++)
   {
     let myVar_body = req.body[config[i].column_name]
     if (typeof myVar_body !== 'undefined')
      {
        console.log(config[i].column_name)
        console.log(myVar_body)

        //Handling for JSON Type
        if (config[i].data_type === COL_JSON_TYPE) 
        {
          req.body[config[i].column_name] = `parse_json('${req.body[config[i].column_name]}')`;
        }

        //Handling for Array Type
        if (config[i].data_type === COL_ARRAY_TYPE) 
        {
          const values = req.body[config[i].column_name].replace(/[\[\]]+/g, "").replace(/"+/g, "'");
          let arrayConstruct = "array_construct(";
          values.split("").forEach((val) => (arrayConstruct += val));
          req.body[config[i].column_name] = arrayConstruct + ")";
        }
        
        //Handling for DateTime
        if (config[i].data_type === COL_DATE_TYPE) 
        {
          req.body[config[i].column_name] = "'" + req.body[config[i].column_name] + "'";
        }

        //Handling for String
        if (config[i].data_type === COL_STRING_TYPE) 
        {
          req.body[config[i].column_name] = req.body[config[i].column_name] ? "'" + req.body[config[i].column_name].replace(/'/g, "''") + "'" : null;
        }   
        //Handling for bOOLEAN
        if (config[i].data_type === COL_BOOLEAN_TYPE) 
        {
          //print('COMING HERE' , config[i].data_type)
          req.body[config[i].column_name] = req.body[config[i].column_name] ? "'" + req.body[config[i].column_name].replace(/'/g, "''") + "'" : null
        }     
      }

      let myVar_col = req.query[config[i].column_name]
      if (typeof myVar_col !== 'undefined')
       {
         console.log(config[i].column_name)
         console.log(myVar_col)
 
         //Handling for JSON Type
         if (config[i].data_type === COL_JSON_TYPE) 
         {
          req.query[config[i].column_name] = `parse_json('${req.query[config[i].column_name]}')`;
         }
 
         //Handling for Array Type
         if (config[i].data_type === COL_ARRAY_TYPE) 
         {
           const values = req.query[config[i].column_name] ? req.query[config[i].column_name].replace(/[\[\]]+/g, "").replace(/"+/g, "'") : null;
           let arrayConstruct = "array_construct(";
           values.split("").forEach((val) => (arrayConstruct += val));
           req.query[config[i].column_name] = arrayConstruct + ")";
         }
         
         //Handling for DateTime
         if (config[i].data_type === COL_DATE_TYPE) 
         {
          req.query[config[i].column_name] = "'" + req.query[config[i].column_name] + "'";
         }
 
         //Handling for String
         if (config[i].data_type === COL_STRING_TYPE) 
         {
          //req.query[config[i].column_name] = "'" + req.query[config[i].column_name].replace(/'/g, "''") + "'";
          req.query[config[i].column_name] = config[i].form_editor_ui === "time" ? "'" + moment(req.query[config[i].column_name], config[i].format).format(timeFormat) + "'" : "'" + req.query[config[i].column_name].replace(/'/g, "''") + "'";
         }
        //Handling for bOOLEAN
        if (config[i].data_type === COL_BOOLEAN_TYPE) 
        {
          //print('COMING HERE' , config[i].data_type)
          req.query[config[i].column_name] = req.query[config[i].column_name] ? "'" + req.query[config[i].column_name].replace(/'/g, "''") + "'" : null;
        }         
       }

   }

   const updateKeys = Object.keys(body);
   const updateQuery123 = `UPDATE  ${table} SET  WHERE `;
  
   let query_set = ''
   console.log("COMING HYER GANESH 123")
   const entries_body = Object.entries(body);
   for (let i = 0; i < entries_body.length; i++) {
     const [key, value] = entries_body[i];
     //console.log(`${key} = ${value}`);
     query_set += `${key} = ${value}`;
     if (i !== entries_body.length - 1) {
       query_set += ",";
     }
   }
   

  let query_where = ''
  const entries_cols = Object.entries(cols);
  for (let i = 0; i < entries_cols.length; i++) {
    const [key, value] = entries_cols[i];
    //console.log(`${key} = ${value}`);
    query_where += `${key} = ${value}`;
    if (i !== entries_cols.length - 1) {
      query_where += " AND ";
    }
  }

  console.log(`UPDATE  ${table} SET ${query_set} WHERE ${query_where} `)
  console.log("query_set: \n\n\n ", query_set)
  //}
  
  //if (typeof db.client.config.client === "function") {
    const updateQuery = `UPDATE  ${table} SET ${query_set} WHERE ${query_where};`;
    console.log(">>", updateQuery);
    const result =  await db.raw(updateQuery).timeout(TIMEOUT);
    count = result.rows[0]['number of rows updated'];
    console.log(count)
  } else {
    count = await db(table)
    .update(body)
    .where((qb) => {
      let queryStringKeys = Object.keys(cols);
      queryStringKeys.map((key, idx) => {
        if (idx === 0) {
          return qb.where(key, cols[key]);
        } else {
          qb.andWhere(key, cols[key]);
        }
      });
    });
  }
    // const count = await db(table)
    //   .update(body)
    //   .where((qb) => {
    //     let queryStringKeys = Object.keys(cols);
    //     queryStringKeys.map((key, idx) => {
    //       if (idx === 0) {
    //         return qb.where(key, cols[key]);
    //       } else {
    //         qb.andWhere(key, cols[key]);
    //       }
    //     });
    //   });
    winston.info_update(req, count);
    return res.status(200).json({ count });
  } catch (e) {
    winston.info(req, e);
    res.status(500).json({ message: !db ? `Error connecting to ${app}` : e.message });
  } finally {
    if (db) db.destroy();
  }
};

exports.uploadTableDataWithCsv = async (req, res) => {
  // Add Entitlement Checking
  const { table, app } = req.params;
  const CRUDType = req.body.type;
  const csv = req.file.buffer.toString("utf-8");
  let errors = [];
  let linex = 1;
  const { nameID } = req.user;

  let columnConfig = [];
  let validationConfig = [];
  let output = [];
  let rowIdentifier = [];
  // let crudOperationKey = CRUD_OPERATION_COL_NAME;
  let uniqueRows = [];
  let db;

  res.writeHead(200, {
    "Content-Type": "text/plain",
    "Transfer-Encoding": "chunked",
  });
  // let testCount = 1
  try {
    db = await makeConnection(app, req.user.nameID);
    const configQueyy = MasterTableConfig.find({
      table_name: table,
      application_name: app,
    }).toString();
    const validationQuery = TableValidation.find({
      table_name: table,
      application_name: app,
    }).toString();
    const result = await Promise.all([knex.raw(configQueyy), knex.raw(validationQuery)]);
    columnConfig = result[0];
    validationConfig = result[1];
    if (!columnConfig || !columnConfig.length) {
      throw new Error("Master column configuration not found");
    }
    await checkUserPermissions(req, app, table);
  } catch (eCheckUser) {
    winston.info(req, eCheckUser);
    res.write(
      JSON.stringify({
        errors: [...errors, eCheckUser.message],
        insertCount: 0,
        updateCount: 0,
        processed: 0,
        total: -1,
      }) + "\n",
    );
    return res.end();
    // return res.status(500).json({ message: e.message });
  }
  csvParse(csv, { trim: true, columns: true, skip_empty_lines: true })
    .on("error", async function (eOn) {
      winston.error(req, eOn.message);
      res.write(
        JSON.stringify({
          errors: [...errors, eOn.message],
          insertCount: 0,
          updateCount: 0,
          processed: 0,
          total: -2,
        }) + "\n",
      );
      return res.end();
      // return res.status(500).json({ message: e.message });
    })
    .on("readable", async function () {
      console.log("Start")
      console.log(Date.now())
      let record;
      let autoTimestampCol = [];
      let autoTimestampColVal;
      let autoUserCol = [];
      let keys = [];
      let dateKeys = [];
      let notEditableCols = [];
      let notInsertableCols = [];
      let notNullCols = [];
      let columnError = false;
      let format = "DD/MM/YYYY HH:mm:ss";
      let dateFormatError = false;
      let totalRows = this.info.records;

      if (this.info.records > 1000000) {
        errors.push({
          msg: "E0001 - CSV file must not exceed 500 rows",
        });
        this.destroy();
        res.write(
          JSON.stringify({
            errors,
            insertCount: 0,
            updateCount: 0,
            processed: 0, 
            total: totalRows,
          }) + "\n",
        );
        return res.end();
        // return res.status(200).json({ errors });
      }

      while ((record = this.read())) {
        linex++;
        if (record && !columnError) {
          if (linex === 2) {
            keys = Object.keys(record);
            // crudOperationKey = keys.find((val) => val.toUpperCase() === CRUD_OPERATION_COL_NAME);
            // if (!crudOperationKey) {
            //   errors.push({
            //     line: linex, data: "", msg: "No crud operation col found",
            //   });
            //   columnError = true;
            //   continue;
            // }
            columnConfig.map((conf) => {
              const colIdx = keys.indexOf(conf.column_name);
              if (!conf.allow_edit_flag) {
                notEditableCols.push(conf.column_name);
              }
              if (!conf.allow_insert_flag) {
                notInsertableCols.push(conf.column_name);
              }
              if (!conf.allow_null_flag) {
                notNullCols.push(conf.column_name);
              }
              if (conf.data_type === COL_DATE_TYPE) {
                keys.find((key) => {
                  if (conf.column_name === key) {
                    dateKeys.push(key);
                    format = getDateFormat(conf.format).trim();
                    if (!!record[key] && record[key].trim().length > 0 && !moment(record[key], format, true).isValid()) {
                      dateFormatError = true;
                      errors.push({
                        line: linex,
                        msg: `E0002 - For column : ${key}, Date must be in ${format} format`,
                      });
                      return true;
                    }
                    record[key] = transformDate_bulkUpload(record[key], format, db);
                    return true;
                  }
                  return false;
                });
              } else if (conf.data_type === COL_INTEGER_TYPE || conf.data_type === COL_DECIMAL_TYPE) {
                keys.forEach((key) => {
                  if (conf.column_name === key) {
                    if (!record[key]) record[key] = null;
                  }
                });
              }

              //if (colIdx > -1) {
              if (conf.bind_value && conf.bind_value.toLowerCase() === AUTO_USER) {
                autoUserCol.push(conf.column_name);
              }
              if (conf.bind_value && conf.bind_value.toLowerCase() === AUTO_TIMESTAMP) {
                /*{
                autoTimestampCol = conf.column_name;
                if (typeof db.client.config.client === "function") {
                  // if db is snowflake
                  const rawTime = new Date(); // + 8 hrs
                  autoTimestampColVal = moment
                    .utc(new Date(rawTime.getTime() + 28800000))
                    .format(YMDHMS_FORMAT_STRING);
                } else if (conf.format.includes(" ")) {
                  autoTimestampColVal = new Date();
                } else {
                  autoTimestampColVal = moment(new Date(), conf.format).format(DMY_FORMAT_STRING);
                }
              }
              */
                autoTimestampCol.push(conf.column_name);
                if (typeof db.client.config.client === "function") {
                  // if db is snowflake
                  const aa = new Date(); // + 8 hrs
                  autoTimestampColVal = moment.utc(new Date(aa.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING);
                } else if (conf.format.includes(" ")) {
                  if (db.client.config.client === "mssql") {
                    const aa = new Date(); // + 8 hrs
                    autoTimestampColVal = moment.utc(new Date(aa.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING);
                  } else autoTimestampColVal = new Date();
                } else {
                  if (db.client.config.client === "mssql") {
                    const aa = new Date(); // + 8 hrs
                    autoTimestampColVal = moment(moment.utc(new Date(aa.getTime() + 28800000)), conf.format).format(YMDHMS_FORMAT_STRING);
                  } else autoTimestampColVal = moment(new Date(), conf.format).format(DMY_FORMAT_STRING);
                }
              }
              //} else
              if (colIdx == -1 && conf.allow_edit_flag && !conf.hidden_flag) {
                errors.push({
                  line: linex,
                  msg: `E0002 - Column ${conf.column_name} is either missing from the Upload csv or it is not part of the allowed list of editable/insertable columns`,
                });
              }
              if (conf.row_identifier_flag) {
                rowIdentifier.push(conf.column_name);
              }
            });
            keys.forEach((col) => {
              if (col === CRUD_OPERATION_COL_NAME) return;
              const existingCol = columnConfig.find((config) => config.column_name === col);
              if (!existingCol) {
                errors.push({
                  line: linex,
                  msg: `E0003 - Column ${col} does not exist in the Table`,
                });
              }
            });
          } else {
            dateKeys.map((key) => {
              const config = columnConfig.find((col) => col.column_name === key);

              format = getDateFormat(config.format).trim();
              if (!!record[key] && record[key].trim().length > 0 && !moment(record[key], format, true).isValid()) {
                dateFormatError = true;
                errors.push({
                  line: linex,
                  msg: `E0004 - For column : ${key}, Date must be in ${format} format`,
                });
                return;
              }
              //console.log("dateFormatError : " + dateFormatError )
              record[key] = transformDate_bulkUpload(record[key], format, db);
            });
          }
          if (dateFormatError) continue;

          // if (!record[crudOperationKey]) {
          //   errors.push({ line: linex, msg: `no crud operation found` });
          //   continue;
          // }
          // if(record[crudOperationKey].toLowerCase() === CRUD_UPDATE) {
          //   const colsWithValue = notEditableCols.find(col => record[col]);
          //   if(colsWithValue) {
          //     errors.push({line: linex, msg: `edit not alllowed for column ${notEditableCols.join(",")}. keep the column empty.`});
          //     continue;
          //   } else {
          //     notEditableCols.map(col =>  delete record[col]);
          //   }
          // }

          if (CRUDType.toLowerCase() === CRUD_UPSERT) {
            const colsWithValue = notInsertableCols.find((col) => record[col]);
            if (colsWithValue) {
              errors.push({
                line: linex,
                msg: `E0005 - Insert operation is not allowed for these columns : ${notInsertableCols.join(",")}. Ensure that the Column is Empty`,
              });
              continue;
            } else {
              notInsertableCols.map((col) => delete record[col]);
            }
          }

          const uniqueRowIdx = output.findIndex((val) => {
            rowIdentifier.find((identifier) => val[identifier] === record[identifier]);
          });
          if (uniqueRowIdx > -1) {
            errors.push({
              line: linex,
              msg: "E0007 - This is a duplicate row",
            });
            continue;
          }

          if (CRUDType.toLowerCase() === CRUD_UPSERT) {
            if (notNullCols.find((col) => Object.prototype.hasOwnProperty.call(record, col) && !record[col])) {
              errors.push({
                line: linex,
                msg: `E0006 - The following columns are not Nullable : ${notNullCols.join(",")}`,
              });
              continue;
            }
            //console.log("stanley_csv_beforeVal", record)

            const _errors = doValidation(validationConfig, record, linex);
            if (_errors.length) {
              errors = [...errors, ..._errors];
              continue;
            }
            if (CRUDType.toLowerCase() === CRUD_UPSERT) {
              const uniqueObj = rowIdentifier.reduce(
                (accu, curr) => {
                  return { ...accu, ...{ [curr]: record[curr] } };
                },
                { line: linex },
              );
              uniqueRows.push(uniqueObj);
            }
            if (autoUserCol) {
              autoUserCol.forEach((col) => {
                record[col] = nameID;
              });
            }
            if (autoTimestampCol) {
              autoTimestampCol.forEach((col) => {
                record[col] = autoTimestampColVal;
              });
            }
          }
          record.line = linex;
          output.push(record);
        } else {
          errors.push({
            line: linex,
            data: "",
            msg: "E0008 - This row has missing Data",
          });
        }
      }
    })
    .on("end", async function () {
      console.log("End")
      console.log(Date.now())
      let line = 0;
      let db;
      let updateCount = 0;
      let insertCount = 0;
      let totalRows = this.info.records;
      let notEditableInsertableCols = [];
      //let notInsertableCols = [];
      try {
        const app = req.params.app;
        db = await makeConnection(app, req.user.nameID);
        if (app.toLowerCase().includes(MOBIUS_APP_NAME) && output[0]) {
          let appKey = "";
          if (Object.prototype.hasOwnProperty.call(output[0], "APPLICATION")) {
            appKey = "APPLICATION";
          }
          if (Object.prototype.hasOwnProperty.call(output[0], "APPLICATION_NAME")) {
            appKey = "APPLICATION_NAME";
          }
          if (appKey) {
            const userApps = await checkUserEntitlementApps(app, table, req.user.group);
            let listOfNonEntitledApps = [];
            output.forEach((row) => {
              //console.log("Output Row : ", row )
              if (!userApps.find((userApp) => userApp.application_name === row[appKey]))
                if (listOfNonEntitledApps.indexOf(row[appKey]) == -1) listOfNonEntitledApps.push(row[appKey]);
            });
            // console.log(
            //   " listOfNonEntitledApps : ",
            //   listOfNonEntitledApps.length
            // );
            if (listOfNonEntitledApps.length > 0) {
              // throw new Error("your are not entitled to the app");
              errors.push({
                msg: `E0009 - This operation is prohibited becuase you are not entitled to the following Application(s) : ${listOfNonEntitledApps.join(",")}`,
              });
            }
            // TOREMOVE
            // const appEntitled = output.filter(
            //   (rowVal) =>
            //     !userApps.find(
            //       (userApp) => userApp.application_name == rowVal[appKey]
            //     ) && (CRUDType.toLowerCase() === CRUD_INSERT || CRUDType.toLowerCase() === CRUD_UPDATE)
            // );
            // if (appEntitled.length) {
            //   appEntitled.map((entitled) =>
            //     errors.push({ msg: `E0009 - This operation is probihited becuase you are not entitled to this Application : ${entitled[appKey]}`, })
            //   );
            // }
          }
        }

        if (errors.length) {
          // return res.status(200).json({ errors });
          res.write(
            JSON.stringify({
              errors,
              insertCount,
              updateCount,
              processed: insertCount + updateCount,
              total: totalRows,
            }) + "\n",
          );
          return res.end();
        }
        if (!output.length) {
          throw new Error("E0010 - The file is empty");
        }

        /* TOREMOVE?
        if (uniqueRows.length) {
          const existingRows = await db(table).where((qb) => {
            uniqueRows.map((row, index) => {
              const whereObj = rowIdentifier.reduce(
                (accu, curr) => ({ ...accu, ...{ [curr]: row[curr] } }),
                {}
              );
              if (index === 0) {
                return qb.where(whereObj);
              }
              qb.orWhere(whereObj);
            });
          });

          console.log("Existing rows : ", existingRows);

          if (existingRows && existingRows.length) {
            // Handling for Snowflake -> Becuase snowflake returns the column names in lower case
            if (typeof db.client.config.client === "function") {
              existingRows.map((row) =>
                errors.push({
                  msg: `E0011 - The row with the following identifiers: ${rowIdentifier.join(
                    ","
                  )}: ${rowIdentifier
                    .map((v) => row[v.toLowerCase()])
                    .join(",")} already exist in the database`,
                })
              );
            } else {
              // Hnadling for MSSQ + Oracle Db
              //console.log("Existing rows : ", existingRows);
              existingRows.map((row) =>
                errors.push({
                  msg: `E0011 - The row with the following identifiers: ${rowIdentifier.join(
                    ","
                  )}: ${rowIdentifier.map(v => v.toLowerCase())
                    .map((v) => row[v.toLowerCase()])
                    .join(",")} already exist in the database`,
                })
              );
            }
            return res.status(200).json({ errors });
          }
        }
        */

        for (let row of output) {
          line = row.line;
          delete row.line;
          let updateOk = false; // AARON P1
          if (CRUDType.toLowerCase() === CRUD_UPDATE || CRUDType.toLowerCase() === CRUD_UPSERT) {
            try {
              delete row[CRUD_OPERATION_COL_NAME];
              // if (typeof db.client.config.client === "function") {
              //   Object.keys(row).forEach((key) => {
              //     if (!row[key]) {
              //       delete row[key];
              //     }
              //   });
              // }
              columnConfig.map((col) => {
                // Get list of Non Editable, Non Insertable, Non Binded Columns
                if (!col.allow_edit_flag && !col.allow_insert_flag && col.bind_value.toLowerCase() != AUTO_USER && col.bind_value.toLowerCase() != AUTO_TIMESTAMP) {
                  notEditableInsertableCols.push(col.column_name);
                }
                //if (!col.allow_insert_flag) {
                //  notInsertableCols.push(col.column_name);
                //}
                if (col.data_type === COL_DATE_TYPE) {
                  if (!row[col.column_name]) {
                    // false, undefined, 0, ''
                    row[col.column_name] = null;
                  }
                  // else
                  //   row[col.column_name] = new Date(row[col.column_name]);
                } else if (col.data_type === COL_BOOLEAN_TYPE) {
                  if (!row[col.column_name]) row[col.column_name] = null;
                } else if (col.data_type === COL_INTEGER_TYPE || col.data_type === COL_DECIMAL_TYPE) {
                  if (!row[col.column_name]) row[col.column_name] = null;
                } else if (col.data_type === "string" || col.form_editor_ui === "time") {
                  if (!row[col.column_name]) row[col.column_name] = null;
                }
                // //console.log( " row[col.column_name] : "  , row[col.column_name])
                // // console.log(" row[col.column_name].trim() : " , row[col.column_name].trim().length)
                // if(row[col.column_name]) {
                //   if (col.data_type === COL_DATE_TYPE && row[col.column_name].length===0 )  {
                //     row[col.column_name]=null
                //     //console.log( "eherehrrheher : ", row[col.column_name] );
                //   }
                //   //console.log("2342342343 : " , row)
                // }
              });
              console.log("List of Non Editable & Insertable Columns : ", notEditableInsertableCols);
              for (let col of notEditableInsertableCols) {
                delete row[col];
              }
              if (typeof db.client.config.client === "function") {
                console.log("COMING INSIDE FUNCTION >....")
                const body = row;
                const autoTimestampCol = columnConfig.find((config) => config.bind_value.toLowerCase() === AUTO_TIMESTAMP);
                const autoUserCol = columnConfig.find((config) => config.bind_value.toLowerCase() === AUTO_USER);
                const defaultBooleanCol = columnConfig.find((config) => config.form_editor_ui.toLowerCase() === "switch");
                Object.keys(body).forEach((key) => {
                  const config = columnConfig.find((config) => config.column_name === key);
                  const timeFormat = config.format.toLowerCase().includes("ss") ? HHMMSS_FORMAT_STRING : HHMM_FORMAT_STRING;
                  if (body[key] === null) {
                    body[key] = "null";
                    // TOREMOVE delete body[key];
                    // if (!body[key]) {
                    //} else if (config.data_type === "string") {
                    //  body[key] = "'" + body[key] + "'";
                  } else if (config.column_name === key && config.data_type === "string") {
                    body[key] =
                      config.form_editor_ui.toLowerCase() === "time" ? "'" + moment(body[key], config.format).format(timeFormat) + "'" : "'" + body[key].replace(/'/g, "''") + "'";
                  } else if (config.data_type === "datetime") {
                    body[key] = "'" + body[key] + "'";
                  } else if (config.data_type === COL_JSON_TYPE) {
                    body[key] = `parse_json('${body[key]}')`;
                  } else if (config.data_type === COL_ARRAY_TYPE) {
                    const values = body[key].replace(/[\[\]\{\}]+/g, "").replace(/"+/g, "'");
                    let arrayConstruct = "array_construct(";
                    values.split("").forEach((val) => (arrayConstruct += val));
                    body[key] = arrayConstruct + ")";
                  }
                });

                if (autoTimestampCol) {
                  const rawTime = new Date(); // + 8 hrs
                  body[autoTimestampCol.column_name] = "'" + moment.utc(new Date(rawTime.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING) + "'";
                }
                if (autoUserCol) {
                  body[autoUserCol.column_name] = "'" + nameID + "'";
                }
                if (defaultBooleanCol && !body.hasOwnProperty(defaultBooleanCol.column_name)) body[defaultBooleanCol.column_name] = 0;
               
                console.log()
                let query_set = ''
                const entries_body = Object.entries(body);
                console.log(entries_body)
                for (let i = 0; i < entries_body.length; i++) {
                  const [key, value] = entries_body[i];
                  console.log(`${key} = ${value}`);
                  query_set += `${key} = ${value}`;
                  if (i !== entries_body.length - 1) {
                    query_set += ",";
                  }
                }
                
                let query_where = ''
               //const entries_cols = Object.entries(cols);
               for (let i = 0; i < rowIdentifier.length; i++) {
                 const key = rowIdentifier[i]
                 const value = row[rowIdentifier[i]]
                 console.log(`${key} = ${value}`);
                 query_where += `${key} = ${value}`;
                 if (i !== rowIdentifier.length - 1) {
                   query_where += " AND ";
                 }
               }
             
               console.log(`UPDATE  ${table} SET ${query_set} WHERE ${query_where} `)
               const updateQuery = `UPDATE  ${table} SET ${query_set} WHERE ${query_where} `
                winston.info(req, req.user.nameID);
                //dbResponse = await db.raw(updateQuery).timeout(TIMEOUT);
                const result =  await db.raw(updateQuery).timeout(TIMEOUT);
                let cnt = 0
                cnt = result.rows[0]['number of rows updated'];
                if (cnt == 0) {
                  // AARON P1 - try insert
                  // errors.push({ line, msg: "E0012 - Unable to find this record in the Database", });
                } else {
                  updateCount++;
                  updateOk = true; // AARON P1
                }
              }
              else {
                dbResponse = await db(table)
                .update(row)
                .where((qb) => {
                  // TOREMOVE
                  // columnConfig.map((col) => {
                  //   if (col.data_type === COL_DATE_TYPE) {
                  //     if (!row[col.column_name]) { // false, undefined, 0, ''
                  //       row[col.column_name] = null
                  //     }
                  //     // else
                  //     //   row[col.column_name] = new Date(row[col.column_name]);
                  //   }
                  //   // //console.log( " row[col.column_name] : "  , row[col.column_name])
                  //   // // console.log(" row[col.column_name].trim() : " , row[col.column_name].trim().length)
                  //   // if(row[col.column_name]) {
                  //   //   if (col.data_type === COL_DATE_TYPE && row[col.column_name].length===0 )  {
                  //   //     row[col.column_name]=null
                  //   //     //console.log( "eherehrrheher : ", row[col.column_name] );
                  //   //   }
                  //   //   //console.log("2342342343 : " , row)
                  //   // }
                  // });
                  rowIdentifier.map((identifier, idx) => {
                    //console.log("identifier", identifier);
                    //console.log("row_identifier", row[identifier]);
                    if (idx === 0) qb.where(identifier, row[identifier]);
                    else qb.andWhere(identifier, row[identifier]);
                  });
                });

                console.log("dbResponse", dbResponse);
                if (!dbResponse) {
                  // AARON P1 - try insert
                  // errors.push({ line, msg: "E0012 - Unable to find this record in the Database", });
                } else {
                  updateCount++;
                  updateOk = true; // AARON P1
                }
              }
              
              // if (typeof db.client.config.client === "function") 
              // { 
              //   //console.log("dbResponse", dbResponse);
              //   if (cnt == 0) {
              //     // AARON P1 - try insert
              //     // errors.push({ line, msg: "E0012 - Unable to find this record in the Database", });
              //   } else {
              //     updateCount++;
              //     updateOk = true; // AARON P1
              //   }
              // }
              // else
              // {
              //   console.log("dbResponse", dbResponse);
              //   if (!dbResponse) {
              //     // AARON P1 - try insert
              //     // errors.push({ line, msg: "E0012 - Unable to find this record in the Database", });
              //   } else {
              //     updateCount++;
              //     updateOk = true; // AARON P1
              //   }
              // }

            } catch (eUpdate) {
              errors.push({
                line,
                msg: "Upload Update Exception: " + eUpdate.message,
              });
            }
          }

          if (CRUDType.toLowerCase() === CRUD_INSERT || (!updateOk && CRUDType.toLowerCase() === CRUD_UPSERT)) {
            try {
              delete row[CRUD_OPERATION_COL_NAME];
              if (typeof db.client.config.client === "function") {
                const body = row;
                // const autoTimestampCol = columnConfig.find((config) => config.bind_value.toLowerCase() === AUTO_TIMESTAMP);
                // const autoUserCol = columnConfig.find((config) => config.bind_value.toLowerCase() === AUTO_USER);
                // const defaultBooleanCol = columnConfig.find((config) => config.form_editor_ui.toLowerCase() === "switch");
                // Object.keys(body).forEach((key) => {
                //   const config = columnConfig.find((config) => config.column_name === key);
                //   const timeFormat = config.format.toLowerCase().includes("ss") ? HHMMSS_FORMAT_STRING : HHMM_FORMAT_STRING;
                //   if (body[key] === null) {
                //     body[key] = "null";
                //     // TOREMOVE delete body[key];
                //     // if (!body[key]) {
                //     //} else if (config.data_type === "string") {
                //     //  body[key] = "'" + body[key] + "'";
                //   } else if (config.column_name === key && config.data_type === "string") {
                //     body[key] =
                //       config.form_editor_ui.toLowerCase() === "time" ? "'" + moment(body[key], config.format).format(timeFormat) + "'" : "'" + body[key].replace(/'/g, "''") + "'";
                //   } else if (config.data_type === "datetime") {
                //     body[key] = "'" + body[key] + "'";
                //   } else if (config.data_type === COL_JSON_TYPE) {
                //     body[key] = `parse_json('${body[key]}')`;
                //   } else if (config.data_type === COL_ARRAY_TYPE) {
                //     const values = body[key].replace(/[\[\]']+/g, "");
                //     let arrayConstruct = "array_construct(";
                //     values.split("").forEach((val) => (arrayConstruct += val));
                //     body[key] = arrayConstruct + ")";
                //   }
                // });

                // if (autoTimestampCol) {
                //   const rawTime = new Date(); // + 8 hrs
                //   body[autoTimestampCol.column_name] = "'" + moment.utc(new Date(rawTime.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING) + "'";
                // }
                // if (autoUserCol) {
                //   body[autoUserCol.column_name] = "'" + nameID + "'";
                // }
                // if (defaultBooleanCol && !body.hasOwnProperty(defaultBooleanCol.column_name)) body[defaultBooleanCol.column_name] = 0;
                const insertKeys = Object.keys(body);
                const insertValues = Object.values(body);
                const insertQuery = `INSERT INTO ${table} (${insertKeys.join(", ")}) select ${insertValues.join(", ")}`;
                // console.log('???', body, insertKeys, insertValues, insertQuery)
                winston.info(req, req.user.nameID);
                await db.raw(insertQuery).timeout(TIMEOUT);
                insertCount++;
              } else {
                // const insertQuery = db(table).insert(row).toString();
                //console.log('>>>', insertQuery)
                winston.info(req, req.user.nameID);
                await db(table).insert(row).timeout(TIMEOUT);
                insertCount++;
              }
            } catch (errInsert) {
              errors.push({
                line,
                msg: "Upload Insert Exception: " + errInsert.message,
              });
            }
          }
          if (CRUDType.toLowerCase() === CRUD_DELETE) {
            try {
              delete row[CRUD_OPERATION_COL_NAME];
              // const deleteQuery = db(table)
              //   .del()
              //   .where((qb) => {
              //     rowIdentifier.map((identifier, idx) => {
              //       if (idx === 0) {
              //         qb.where(identifier, row[identifier]);
              //       } else {
              //         qb.andWhere(identifier, row[identifier]);
              //       }
              //     });
              //   })
              //   .toString();
              // winston.info(req, req.user.nameID);
              await db(table)
                .del(row)
                .where((qb) => {
                  columnConfig.map((col) => {
                    //console.log(row[col.column_name])
                    if (col.data_type === COL_DATE_TYPE && row[col.column_name]) {
                      row[col.column_name] = new Date(row[col.column_name]);
                    }
                  });
                  rowIdentifier.map((identifier, idx) => {
                    if (idx === 0) qb.where(identifier, row[identifier]);
                    else {
                      qb.andWhere(identifier, row[identifier]);
                    }
                  });
                });
            } catch (errDelete) {
              errors.push({
                line,
                msg: "Upload Delete Exception: " + errDelete.message,
              });
            }
          }

          if (![CRUD_INSERT, CRUD_UPDATE, CRUD_UPSERT, CRUD_DELETE].includes(CRUDType.toLowerCase())) {
            errors.push({
              line,
              msg: "E0013 - Crud operation not found for this row",
            });
          }
          res.write(
            JSON.stringify({
              insertCount,
              updateCount,
              processed: insertCount + updateCount,
              total: totalRows,
            }) + "\n",
          );
        }
      } catch (eOnEnd) {
        errors.push({ line, msg: "Upload OnEnd Execption: " + eOnEnd.message });
        //console.log(">>>>> eOnEnd", eOnEnd);
        // if (!db) res.status(500).json({ message: `E0014 - Error connecting to ${app}`, });
      } finally {
        if (db) db.destroy();
      }
      res.write(
        JSON.stringify({
          errors,
          insertCount,
          updateCount,
          processed: insertCount + updateCount,
          total: totalRows,
        }) + "\n",
      );
      return res.end();
    });
};

exports.addTableRow = async (req, res) => {
  let db = "";
  try {
    db = await makeConnection(req.params.app, req.user.nameID);
    const { table, app } = req.params;
    await checkUserPermissions(req, app, table);

    const { body } = req;
    const keys = Object.keys(body);
    // Entitlement check for Apps with MOBIUS_CFG_* naming convention
    if (app.toLowerCase().includes(MOBIUS_APP_NAME)) {
      let appKey = "";
      if (Object.prototype.hasOwnProperty.call(body, "APPLICATION")) {
        appKey = "APPLICATION";
      }
      if (Object.prototype.hasOwnProperty.call(body, "APPLICATION_NAME")) {
        appKey = "APPLICATION_NAME";
      }
      if (appKey) {
        const userApps = await checkUserEntitlementApps(app, table, req.user.group);
        const appEntitled = userApps.find((userApp) => userApp.application_name === body[appKey]);
        if (!appEntitled) {
          throw new Error(`E0009 - This operation is prohibited becuase you are not entitled to this Application : ${body[appKey]}`);
        }
      }
    }

    // Retrieve Master config
    const config = await MasterTableConfig.find((qb) => {
      qb.where({ table_name: table }).andWhere({ application_name: app });
    });
    // Return error when there is no Config found
    if (!config.length) return res.status(500).json({ operation: "insert", message: "config not found" });

    config.map((col) => {
      if (col.form_editor_ui.toLowerCase() === "switch" && typeof db.client.config.client === "function") {
        if (!body.hasOwnProperty(col.column_name)) {
          body[col.column_name] = "null";
        }
      }

      if (col.data_type === COL_BOOLEAN_TYPE) {
        if (body[col.column_name] == undefined) body[col.column_name] = null;
        if (body[col.column_name] === false) body[col.column_name] = "0";
      }

      if (col.data_type === COL_DATE_TYPE) {
        keys.map((key) => {
          if (col.column_name === key) {
            body[key] = transformDate_uiInsert(body[key], col.format, db);
          }
        });
      }
      if (col.data_type === COL_JSON_TYPE) {
        keys.map((key) => {
          if (col.column_name === key) {
            body[key] = `parse_json('${body[key]}')`;
          }
        });
      }
      if (col.data_type === COL_ARRAY_TYPE) {
        keys.map((key) => {
          if (col.column_name === key) {
            const values = body[key].replace(/[\[\]]+/g, "").replace(/"/g, "'");
            let arrayConstruct = "array_construct(";
            values.split("").forEach((val) => (arrayConstruct += val));
            body[key] = arrayConstruct + ")";
          }
        });
      }
      if (col.bind_value && col.bind_value.toLowerCase() === AUTO_USER)
        req.body[col.column_name] = typeof db.client.config.client === "function" ? "'" + req.user.nameID + "'" : req.user.nameID;
      if (col.bind_value && col.bind_value.toLowerCase() === AUTO_TIMESTAMP) {
        if (typeof db.client.config.client === "function") {
          // if db is snowflake
          const aa = new Date(); // + 8 hrs
          req.body[col.column_name] = "'" + moment.utc(new Date(aa.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING) + "'";
        } else if (col.format.includes(" ")) {
          if (db.client.config.client === "mssql") {
            const aa = new Date(); // + 8 hrs
            req.body[col.column_name] = moment.utc(new Date(aa.getTime() + 28800000)).format(YMDHMS_FORMAT_STRING);
          } else req.body[col.column_name] = new Date();
        } else {
          if (db.client.config.client === "mssql") {
            const aa = new Date(); // + 8 hrs
            req.body[col.column_name] = moment(moment.utc(new Date(aa.getTime() + 28800000)), col.format).format(YMDHMS_FORMAT_STRING);
          } else req.body[col.column_name] = moment(new Date(), col.format).format(DMY_FORMAT_STRING);
        }
      }

      if (typeof db.client.config.client === "function") {
        const timeFormat = col.format.toLowerCase().includes("ss") ? HHMMSS_FORMAT_STRING : HHMM_FORMAT_STRING;
        keys.map((key) => {
          if (col.column_name === key && col.data_type === "string") {
            body[key] = col.form_editor_ui === "time" ? "'" + moment(body[key], col.format).format(timeFormat) + "'" : "'" + body[key].replace(/'/g, "''") + "'";
          } else if (col.column_name === key && col.data_type === COL_DATE_TYPE) {
            // Handling -> Included quotes for snowflake before insert
            body[key] = "'" + body[key] + "'";
          }
        });
      }
    });

    // validation here
    const validationConfig = await TableValidation.find({
      table_name: table,
      application_name: app,
    });
    const errors = doValidation(validationConfig, body);
    if (errors.length) throw new Error(JSON.stringify(errors));

    winston.info(req, req.user.nameID);
    if (typeof db.client.config.client === "function") {
      const insertValues = Object.values(body);
      const insertKeys = Object.keys(body);
      const insertQuery = `INSERT INTO ${table} (${insertKeys.join(", ")}) select ${insertValues.join(", ")}`;
      console.log(">>", insertQuery);
      await db.raw(insertQuery).timeout(TIMEOUT);
    } else {
      await db(table).insert(body).timeout(TIMEOUT);
    }
    return res.status(200).json({ message: "success" });
  } catch (e) {
    winston.info(req, e);
    return res.status(500).json({
      operation: "insert",
      message: !db ? `Error connecting to ${app}` : e.message,
    });
  } finally {
    if (db) db.destroy();
  }
};
