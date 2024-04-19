import { CastAddMessage, CastRemoveMessage, Embed, MessageType, Message } from "@farcaster/hub-nodejs";
import { Selectable, sql } from "kysely";
import { jsonObjectFrom } from "kysely/helpers/postgres";
import { buildAddRemoveMessageProcessor } from "../messageProcessor.js";
import { CastEmbedJson, CastRow, executeTakeFirst, executeTakeFirstOrThrow, DBTransaction } from "../db.js";
import { bytesToHex, farcasterTimeToDate, StoreMessageOperation, isAfterTargetTimeToday, 
    isBetweenPeriod, isToday, putKinesisRecords } from "../util.js";
import { AssertionError, HubEventProcessingBlockedError } from "../error.js";
import { PARTITIONS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, useTimePeriodKinesis} from "../env.js";
import AWS from "aws-sdk";
import { Records } from "aws-sdk/clients/rdsdataservice.js";


const { processAdd, processRemove } = buildAddRemoveMessageProcessor<
  CastAddMessage,
  CastRemoveMessage,
  Selectable<CastRow>
>({
  conflictRule: "last-write-wins-remove-trumps",
  addMessageType: MessageType.CAST_ADD,
  removeMessageType: MessageType.CAST_REMOVE,
  withConflictId(message) {
    const hash = message.data.type === MessageType.CAST_ADD ? message.hash : message.data.castRemoveBody?.targetHash;

    return ({ or, and, eb }) => {
      return or([
        and([eb("type", "=", MessageType.CAST_ADD), eb("fid", "=", message.data.fid), eb("hash", "=", hash)]),
        and([
          eb("type", "=", MessageType.CAST_REMOVE),
          eb("fid", "=", message.data.fid),
          sql<boolean>`body ->> 'targetHash' = ${bytesToHex(hash)}`,
        ]),
      ]);
    };
  },
  async getDerivedRow(message, trx) {
    const hash = message.data.type === MessageType.CAST_ADD ? message.hash : message.data.castRemoveBody?.targetHash;

    return await executeTakeFirst(
      trx.selectFrom("casts").select(["deletedAt"]).where("fid", "=", message.data.fid).where("hash", "=", hash),
    );
  },
  async deleteDerivedRow(message, trx, isHubEvent: boolean = false) {
    return await executeTakeFirstOrThrow(
      trx
        .updateTable("casts")
        .where("fid", "=", message.data.fid)
        .where("hash", "=", message.data.castRemoveBody.targetHash)
        .set({ deletedAt: new Date() })
        .returningAll(),
    );
  },
  async mergeDerivedRow(message, deleted, trx, isHubEvent: boolean = false) {
    const {
      hash,
      data: {
        fid,
        timestamp,
        castAddBody: { text, embeds, embedsDeprecated, mentions, mentionsPositions, parentCastId, parentUrl },
      },
    } = message;

    const transformedEmbeds: CastEmbedJson[] = embedsDeprecated?.length
      ? embedsDeprecated.map((url) => ({ url }))
      : embeds.map(({ castId, url }) => {
          if (castId) return { castId: { fid: castId.fid, hash: bytesToHex(castId.hash) } };
          if (url) return { url };
          throw new AssertionError("Neither castId nor url is defined in embed");
        });

    let rootParentHash = null;
    let rootParentUrl = null;
    if (parentCastId) {
      const { parentFidExists, parentCast } = await executeTakeFirstOrThrow(
        trx.selectNoFrom(({ eb, fn, selectFrom }) => [
          eb(selectFrom("fids").select(fn.countAll().as("count")).where("fid", "=", parentCastId.fid), ">", 0).as(
            "parentFidExists",
          ),
          jsonObjectFrom(
            eb
              .selectFrom("casts")
              .select(["fid", "rootParentHash", "rootParentUrl"])
              .where("hash", "=", parentCastId.hash),
          ).as("parentCast"),
        ]),
        () => new AssertionError("No result"),
      );

      if (!parentFidExists) {
        throw new HubEventProcessingBlockedError(
          `Cast reply parent author with FID ${parentCastId.fid} has not yet been registered`,
          {
            blockedOnFid: parentCastId.fid,
            blockedOnHash: parentCastId.hash,
          },
        );
      }

      if (!parentCast) {
        throw new HubEventProcessingBlockedError(`Parent cast ${bytesToHex(parentCastId.hash)} has not yet been seen`, {
          blockedOnHash: parentCastId.hash,
        });
      }

      rootParentHash = parentCast.rootParentHash;
      rootParentUrl = parentCast.rootParentUrl;
    }
    
    let records = [];
    
    let recordsJson = {
      timestamp: farcasterTimeToDate(timestamp),
      deletedAt: deleted ? new Date() : null,
      fid,
      parentFid: parentCastId?.fid || null,
      hash,
      rootParentHash: rootParentHash || parentCastId?.hash || null,
      parentHash: parentCastId?.hash || null,
      rootParentUrl: rootParentUrl || parentUrl || null,
      text,
      embeds: JSON.stringify(transformedEmbeds),
      mentions: JSON.stringify(mentions),
      mentionsPositions: JSON.stringify(mentionsPositions),
    }
    
    records = [
      {
        Data: JSON.stringify(recordsJson),
        PartitionKey: "CAST_ADD",
      },
    ];
    console.log("isTodayCasts: ", isToday(farcasterTimeToDate(timestamp)));
    // if (isAfterTargetTimeToday(farcasterTimeToDate(timestamp)) || (useTimePeriodKinesis && isBetweenPeriod(farcasterTimeToDate(timestamp))))  {
    if(isToday(farcasterTimeToDate(timestamp))) {
      console.log(`push kinesis start`);
      await putKinesisRecords(records, "farcaster-stream");
      records = [
        {
          Data: JSON.stringify(recordsJson),
          PartitionKey: bytesToHex(hash),
        },
      ];
      await putKinesisRecords(records, "farcaster-casts-stream");
      console.log(`push kinesis end`);
    }

    return await executeTakeFirstOrThrow(
      trx
        .insertInto("casts")
        .values({
          timestamp: farcasterTimeToDate(timestamp),
          deletedAt: deleted ? new Date() : null,
          fid,
          parentFid: parentCastId?.fid || null,
          hash,
          rootParentHash: rootParentHash || parentCastId?.hash || null,
          parentHash: parentCastId?.hash || null,
          rootParentUrl: rootParentUrl || parentUrl || null,
          text,
          embeds: JSON.stringify(transformedEmbeds),
          mentions: JSON.stringify(mentions),
          mentionsPositions: JSON.stringify(mentionsPositions),
        })
        .onConflict((oc) =>
          oc
            .$call((qb) => (PARTITIONS ? qb.columns(["hash", "fid"]) : qb.columns(["hash"])))
            .doUpdateSet({
              // If this is a delete, only update deletedAt if it's not already set
              deletedAt: deleted ? (eb) => eb.fn.coalesce("casts.deletedAt", "excluded.deletedAt") : null,
            }),
        )
        .returningAll(),
    );
  },
  async onAdd({ data: cast, isCreate, skipSideEffects, trx }) {
    // Update any other derived data

    if (!skipSideEffects) {
      // Trigger any one-time side effects (push notifications, etc.)
    }
  },
  async onRemove({ data: cast, skipSideEffects, trx }) {
    // Update any other derived data in response to removal

    if (!skipSideEffects) {
      // Trigger any one-time removal side effects (push notifications, etc.)
    }
  },
});


export { processAdd as processCastAdd, processRemove as processCastRemove };
