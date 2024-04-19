import { UserNameProof, UsernameProofMessage, bytesToUtf8String } from "@farcaster/hub-nodejs";
import { DBTransaction } from "../db.js";
import { StoreMessageOperation, farcasterTimeToDate, putKinesisRecords } from "../util.js";

import AWS from 'aws-sdk';
import { Records } from "aws-sdk/clients/rdsdataservice.js";
import {AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY } from "../env.js";




export const processUserNameProofMessage = async (
  message: UsernameProofMessage,
  operation: StoreMessageOperation,
  trx: DBTransaction,
  isHubEvent: boolean = false
) => {
  const proof = message.data.usernameProofBody;
  if (operation === "merge") {
    await processUserNameProofAdd(proof, trx, isHubEvent);
  } else {
    await processUserNameProofRemove(proof, trx, isHubEvent);
  }
};

export const processUserNameProofAdd = async (proof: UserNameProof, trx: DBTransaction, isHubEvent: boolean = false) => {
  const username = bytesToUtf8String(proof.name)._unsafeUnwrap();
  const timestamp = farcasterTimeToDate(proof.timestamp);

  // A removal can also be represented as a transfer to FID 0
  if (proof.fid === 0) {
    return processUserNameProofRemove(proof, trx, isHubEvent);
  }
  
  let records = [];
    
  let recordsJson = {
    timestamp,
    fid: proof.fid,
    type: proof.type,
    username,
    signature: proof.signature,
    owner: proof.owner,
  }
  
  records = [
    {
      Data: JSON.stringify(recordsJson),
      PartitionKey: "USERNAME_PROOFS_ADD",
    },
  ];
  
  let records2Json = {
      registeredAt: timestamp,
      fid: proof.fid,
      type: proof.type,
      username,
      deletedAt: proof.fid === 0 ? timestamp : null,
  }
  
  let records2 = [
    {
      Data: JSON.stringify(recordsJson),
      PartitionKey: "FNAMES_ADD",
    },
  ];
  // console.log(`push kinesis start`);
  // await putKinesisRecords(records, "farcaster-stream");
  // console.log(`push kinesis end`);

  await trx
    .insertInto("usernameProofs")
    .values({
      timestamp,
      fid: proof.fid,
      type: proof.type,
      username,
      signature: proof.signature,
      owner: proof.owner,
    })
    .onConflict((oc) =>
      oc
        .columns(["username", "timestamp"])
        .doUpdateSet({ owner: proof.owner, fid: proof.fid, deletedAt: null, updatedAt: new Date() }),
    )
    .execute();

  
  // console.log(`push kinesis start`);
  // await putKinesisRecords(records2, "farcaster-stream");
  // console.log(`push kinesis end`);
  await trx
    .insertInto("fnames")
    .values({
      registeredAt: timestamp,
      fid: proof.fid,
      type: proof.type,
      username,
      deletedAt: proof.fid === 0 ? timestamp : null, // Sending to FID 0 is treated as delete
    })
    .onConflict((oc) => oc.column("fid").doUpdateSet({ username, updatedAt: new Date() }))
    .execute();
};

export const processUserNameProofRemove = async (proof: UserNameProof, trx: DBTransaction, isHubEvent: boolean = false) => {
  const username = bytesToUtf8String(proof.name)._unsafeUnwrap();
  const now = new Date();

  await trx
    .updateTable("usernameProofs")
    .where("username", "=", username)
    .where("timestamp", "=", farcasterTimeToDate(proof.timestamp))
    .set({ deletedAt: now, updatedAt: now })
    .execute();

  await trx.updateTable("fnames").where("username", "=", username).set({ deletedAt: now, updatedAt: now }).execute();
};
