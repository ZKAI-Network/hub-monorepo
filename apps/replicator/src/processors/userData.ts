import { UserDataAddMessage } from "@farcaster/hub-nodejs";
import { DBTransaction, execute } from "../db.js";
import { farcasterTimeToDate, putKinesisRecords } from "../util.js";

import AWS from "aws-sdk";
import { Records } from "aws-sdk/clients/rdsdataservice.js";
import {AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY } from "../env.js";


export const processUserDataAdd = async (message: UserDataAddMessage, trx: DBTransaction, isHubEvent: boolean = false) => {
  const now = new Date();
  
  
  let records = [];
    
  let recordsJson = {
    timestamp: farcasterTimeToDate(message.data.timestamp),
    fid: message.data.fid,
    hash: message.hash,
    type: message.data.userDataBody.type,
    value: message.data.userDataBody.value,
  }
  
  records = [
    {
      Data: JSON.stringify(recordsJson),
      PartitionKey: "USER_DATA_ADD",
    },
  ];
  // console.log(`push kinesis start`);
  // await putKinesisRecords(records, "farcaster-stream");
  // console.log(`push kinesis end`);
  
  await execute(
    trx
      .insertInto("userData")
      .values({
        timestamp: farcasterTimeToDate(message.data.timestamp),
        fid: message.data.fid,
        hash: message.hash,
        type: message.data.userDataBody.type,
        value: message.data.userDataBody.value,
      })
      .onConflict((oc) =>
        oc
          .columns(["fid", "type"])
          .doUpdateSet(({ ref }) => ({
            hash: ref("excluded.hash"),
            timestamp: ref("excluded.timestamp"),
            value: ref("excluded.value"),
            updatedAt: now,
          }))
          .where(({ or, eb, ref }) =>
            // Only update if a value has actually changed
            or([
              eb("excluded.hash", "!=", ref("userData.hash")),
              eb("excluded.timestamp", "!=", ref("userData.timestamp")),
              eb("excluded.value", "!=", ref("userData.value")),
              eb("excluded.updatedAt", "!=", ref("userData.updatedAt")),
            ]),
          ),
      ),
  );
};
