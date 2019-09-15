require('dotenv').config();

const fs = require('fs');
const cashaddr = require('cashaddrjs');
const GrpcClient = require('grpc-bchrpc-node').GrpcClient;
const MongoClient = require('mongodb').MongoClient;
const fountainhead = require('fountainhead-core');
const bitcore = fountainhead.bitcore;

let grpc = new GrpcClient();


// https://github.com/gcash/bchd/blob/8717f0400dcba02b041cacd4211e2a349d4c1bbf/chaincfg/params.go#L274
let utxo_block_height =  582680;
const utxo_checkpoint_path = './utxo-checkpoints/QmXkBQJrMKkCKNbwv4m5xtnqwU9Sq7kucPigvZW8mWxcrv';



const pk_script_to_address = (pk_script) => {
    // P2PKH
    if (pk_script.length > 5
     && pk_script[0] === 0x76 // OP_DUP
     && pk_script[1] === 0xA9 // OP_HASH160
     && pk_script.length === 3+pk_script[2]+2 // pk_script[2] holds length of sig
    ) {
        return cashaddr.encode('bitcoincash', 'P2PKH', pk_script.slice(3, -2));
    }

    // P2SH
    else
    if (pk_script.length === 23
     && pk_script[0]  === 0xA9 // OP_HASH160
     && pk_script[22] === 0x87 // OP_EQUAL
    ) {
        return cashaddr.encode('bitcoincash', 'P2SH', pk_script.slice(1, 21)); 
    }

    return null;
}


let db = null;
let mongo = null;
new Promise((resolve, reject) => MongoClient.connect(
    process.env.MONGO_URL,
    {
        useNewUrlParser: true,
        useUnifiedTopology: true
    },
    (err, client) => {

    if (err) {
        console.error(err);
        process.exit(1);
    }

    db = client.db(process.env.MONGO_DB);
    mongo = client;
    resolve();
}))
/*
.then(() => {
    const stats = fs.statSync(utxo_checkpoint_path);
    const stream = fs.createReadStream(utxo_checkpoint_path);

    let total_bytes_read = 0;

    let chunk_collector = Buffer.from([]);
    const process_chunks = () => {
        let items = [];

        let idx = 0;

        do {
            const buf = chunk_collector.slice(idx, idx+52);
            if (buf.length < 52) {
                // need to read another chunk
                break;
            }

            const prev_tx_id   = buf.slice(0, 32);
            const prev_out_idx = buf.readUInt32LE(32);
            const is_coinbase  = buf[36+3] & 0x01; // TODO broken
            buf[36+3] &= 0xFE;
            const height       = buf.readUInt32LE(36);
            const value        = buf.readBigUInt64LE(40);
            const script_len   = buf.readInt32LE(48);

            const pk_script = chunk_collector.slice(idx+52, idx+52+script_len);
            if (pk_script.length < script_len) {
                // need to read another chunk
                break;
            }

            idx              += 52 + script_len;
            total_bytes_read += 52 + script_len;

            let address = pk_script_to_address(pk_script);


            // console.log(prev_tx_id.toString('hex'), prev_out_idx, is_coinbase, height, value, address, pk_script.toString('hex'));
            items.push({
                prev_tx_id:   prev_tx_id.toString('hex'),
                prev_out_idx: prev_out_idx,
                is_coinbase:  is_coinbase,
                height:       height,
                value:        value,
                address:      address,
                pk_script:    pk_script.toString('hex'),
            });
        } while (idx < chunk_collector.length);

        chunk_collector = chunk_collector.slice(idx);

        if (items.length > 0) {
            (async () => {
                const insert_status = await db.collection('utxos').insertMany(items, { ordered: false });
                console.log('insert', insert_status.insertedCount);
            })();
        }
    };

    stream.on('data', (chunk) => {
        chunk_collector = Buffer.concat([chunk_collector, chunk]);
        process_chunks();

        const progress = total_bytes_read / stats.size;
        console.log(progress);
    });

    stream.on('end', () => {
        process_chunks();
        console.log('done');
    });
})
*/
.then(() => {
    (async () => {
        console.time('building indexes');

        let tasks = [];
        [
            'prev_tx_id',
            'prev_out_idx',
            'height',
            'address',
            'pk_script'
        ].forEach((k) => {
            tasks.push(new Promise(async (resolve, reject) => {
                console.time(`create index on ${k}`);
                await db.collection('utxos').createIndex({ [k]: 1 });
                console.timeEnd(`create index on ${k}`);
                resolve();
            }));
        });

        Promise.all(tasks).then(async () => {
            console.timeEnd('building indexes');

            const index_info = await db.collection('utxos').indexInformation({ full: true });
            console.log('index_info', index_info);

            resolve();
        });
    })();

    /*
    db.collection('utxos').createIndexes([
        { 'key': { prev_tx_id: 1} },
        { 'key': { prev_out_idx: 1} },
        { 'key': { height:     1} },
        { 'key': { address:    1} },
        { 'key': { pk_script:  1} }
    ]);
    */
})
.then(() => {

    grpc.getRawBlock({ index: 100 })
    .then((res) => {
        const data = Buffer.from(res.getBlock_asU8()).toString('hex');
        const block = bitcore.Block.fromString(data);

        let insertions  = new Map();
        for (const tx of block.transactions) {
            for (let i=0; i<tx.outputs.length; ++i) {
                const output = tx.outputs[i];
                const script_buf = output.script.toBuffer();

                insertions.set(tx.hash+':'+i, {
                    prev_tx_id:   tx.hash,
                    prev_out_idx: i,
                    is_coinbase:  false,
                    height:       100000,
                    value:        output.satoshis,
                    pk_script:    script_buf.toString('hex'),
                    address:      pk_script_to_address(script_buf),
                });
            }
        }

        let deletions = [];
        for (const tx of block.transactions) {
            for (const input of tx.inputs) {
                const prev_tx_id = input.prevTxId.toString('hex');
                insertions.delete(prev_tx_id+':'+input.outputIndex);
                deletions.push({
                    prev_tx_id: prev_tx_id,
                    prev_out_idx: input.outputIndex
                });
            }
        }

        (async () => {
            let total_deleted = 0;
            for (const utxo of deletions) {
                const delete_query = {
                    prev_tx_id:   utxo.prev_tx_id,
                    prev_out_idx: utxo.prev_out_idx,
                };

                console.log('delete_query', delete_query);

                const delete_status = await db.collection('utxos').deleteOne(delete_query);
                total_deleted += delete_status.deletedCount;
                console.log('deleted', total_deleted);
            }

            console.log('deleted', total_deleted);
        })();

        (async () => {
            // console.log([...insertions.values()]);
            const insert_status = await db.collection('utxos').insertMany([...insertions.values()], { ordered: false });
            console.log('inserted', insert_status.insertedCount);
        })();
    });
});



/*
let txid = "11556da6ee3cb1d14727b3a8f4b37093b6fecd2bc7d577a02b4e98b7be58a7e8";
grpc.getBlockInfo({ index: 1 })
.then((res) => {
    console.log(Buffer.from(res.getInfo().getHash_asU8().reverse()).toString('hex'));
    // console.log(Buffer.from(res.getTransaction_asU8()).toString('hex'));
});

grpc.getRawBlock({ index: 100000 })
.then((res) => {
    console.log(Buffer.from(res.getBlock_asU8()).toString('hex'));
});

*/
