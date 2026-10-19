const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');

const [binary, backend = 'epoll', daemonBinary, mode = 'rdma'] = process.argv.slice(2);
if (mode === 'probe' && process.env.KVSTORE_TEST_UPROBE !== '1') {
    console.log('SKIP: set KVSTORE_TEST_UPROBE=1 with BPF privileges to run real probe capture');
    process.exit(0);
}
const address = process.env.KVSTORE_RDMA_ADDRESS;
if (!address) {
    console.log('SKIP: KVSTORE_RDMA_ADDRESS is required');
    process.exit(0);
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const encode = words => Buffer.concat([
    Buffer.from(`*${words.length}\r\n`),
    ...words.flatMap(word => {
        const data = Buffer.isBuffer(word) ? word : Buffer.from(String(word));
        return [Buffer.from(`$${data.length}\r\n`), data, Buffer.from('\r\n')];
    }),
]);

async function unusedPort() {
    const listener = net.createServer();
    await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
    const port = listener.address().port;
    await new Promise(resolve => listener.close(resolve));
    return port;
}

async function connect(port) {
    return new Promise((resolve, reject) => {
        const socket = net.connect(port, '127.0.0.1');
        socket.once('connect', () => resolve(socket));
        socket.once('error', reject);
    });
}

function client(socket) {
    let pending = Buffer.alloc(0);
    let wake;
    socket.on('data', bytes => {
        pending = Buffer.concat([pending, bytes]);
        if (wake) wake();
    });
    socket.on('error', () => {});
    return {
        socket,
        async command(...words) {
            socket.write(encode(words));
            const deadline = Date.now() + 5000;
            while (Date.now() < deadline) {
                const end = pending.indexOf('\r\n');
                if (end >= 0) {
                    const header = pending.subarray(0, end).toString();
                    let bytes = end + 2;
                    let value = header;
                    if (header.startsWith('$') && header !== '$-1') {
                        const length = Number(header.slice(1));
                        bytes += length + 2;
                        if (pending.length < bytes) {
                            await new Promise(resolve => { wake = resolve; setTimeout(resolve, 20); });
                            continue;
                        }
                        value = pending.subarray(end + 2, end + 2 + length);
                    }
                    pending = pending.subarray(bytes);
                    return value;
                }
                await new Promise(resolve => { wake = resolve; setTimeout(resolve, 20); });
            }
            throw new Error('RESP response timeout');
        },
    };
}

async function eventually(action) {
    const deadline = Date.now() + 10000;
    let error;
    while (Date.now() < deadline) {
        try { return await action(); } catch (failure) { error = failure; }
        await delay(50);
    }
    throw error || new Error('Condition timeout');
}

function rdmaDescriptors(child) {
    const directory = `/proc/${child.pid}/fd`;
    return fs.readdirSync(directory).flatMap(fd => {
        try {
            const target = fs.readlinkSync(path.join(directory, fd));
            return /infiniband|rdma_cm/.test(target) ? [target] : [];
        } catch (error) {
            if (error.code === 'ENOENT') return [];
            throw error;
        }
    }).sort();
}

async function main() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kvstore-replication-test-'));
    const children = [];
    const sockets = [];
    const launch = (name, args, executable = binary) => {
        const cwd = path.join(root, name);
        fs.mkdirSync(cwd);
        const output = fs.openSync(path.join(cwd, 'server.log'), 'w');
        // Probe mode inherits the master's backend; legacy RDMA fixtures use epoll.
        const child = spawn(executable, ['--multiplexer', mode === 'probe' && name === 'master' ? backend : 'epoll',
            ...(executable === binary ? ['--rdma-device', 'siw0'] : []), ...args],
            { cwd, stdio: ['ignore', output, output] });
        fs.closeSync(output);
        children.push(child);
        return child;
    };
    const stop = child => new Promise(resolve => {
        if (child.exitCode !== null || child.signalCode !== null) return resolve();
        child.once('exit', resolve);
        child.kill('SIGTERM');
    });
    const attach = async port => {
        const socket = await eventually(() => connect(port));
        sockets.push(socket);
        return client(socket);
    };
    try {
        const value = Buffer.from([0, 13, 10, 255, 42]);
        if (mode === 'probe') {
        const masterPort = await unusedPort();
        const replicaPort = await unusedPort();
        const rdmaPort = await unusedPort();
        const daemonPort = await unusedPort();
        assert.ok(daemonBinary, 'actual repld executable is required');
        const masterProcess = launch('master', ['--port', String(masterPort), '--replication-port', String(rdmaPort),
            '--replication-ip', address, '--replication-daemon', `127.0.0.1:${daemonPort}`,
            '--repld', daemonBinary]);
        const replicaProcess = launch('replica', ['--port', String(replicaPort)]);
        const master = await attach(masterPort);
        const replica = await attach(replicaPort);
        const idleRdmaDescriptors = rdmaDescriptors(replicaProcess);
        const idleMasterDescriptors = rdmaDescriptors(masterProcess);
        assert.ok(idleRdmaDescriptors.length > 0, 'RDMA resource descriptors must be observable');
        assert.equal(await master.command('SET', 'snapshot-key', 'seed'), '+OK');
        const ready = await connectDaemon(daemonPort);
        ready.destroy();
        const daemonPid = Number(fs.readFileSync(
            `/proc/${masterProcess.pid}/task/${masterProcess.pid}/children`, 'utf8').trim());
        assert.ok(daemonPid > 0, 'master must launch one repld child');
        assert.equal(await replica.command('SLAVEOF', address, rdmaPort), '+OK');
        await eventually(async () => assert.deepEqual(await replica.command('GET', 'snapshot-key'), Buffer.from('seed')));
        // Wait for snapshot completion and the daemon FOLLOW handshake.
        await delay(100);
        assert.equal(await master.command('SET', 'binary-key', value), '+OK');
        await eventually(async () => assert.deepEqual(await replica.command('GET', 'binary-key'), value));
        await eventually(() => assert.deepEqual(rdmaDescriptors(replicaProcess), idleRdmaDescriptors,
            'snapshot RDMA descriptors must be released while TCP replication remains active'));
        await eventually(() => assert.deepEqual(rdmaDescriptors(masterProcess), idleMasterDescriptors,
            'master must also release the inherited RDMA connection after handoff'));
        assert.equal(await master.command('DEL', 'snapshot-key'), ':1');
        await eventually(async () => assert.equal(await replica.command('GET', 'snapshot-key'), '$-1'));

        // These writes travel through the real uprobe and daemon, not injected frames.
        const secondPort = await unusedPort();
        launch('replica-two', ['--port', String(secondPort)]);
        const second = await attach(secondPort);
        assert.equal(await second.command('SLAVEOF', address, rdmaPort), '+OK');
        await eventually(async () => assert.deepEqual(await second.command('GET', 'binary-key'), value));
        await delay(100);
        for (let index = 0; index < 100; ++index) {
            assert.equal(await master.command('SET', `key-${index}`, `value-${index}`), '+OK');
        }
        for (const target of [replica, second]) {
            await eventually(async () => assert.deepEqual(await target.command('GET', 'key-99'), Buffer.from('value-99')));
        }

        // Larger records must survive the uprobe chunk assembly and TCP transport.
        const large = Buffer.alloc(70000, 42);
        assert.equal(await master.command('SET', 'overflow-key', large), '+OK');
        for (const target of [replica, second]) {
            await eventually(async () => {
                const received = await target.command('GET', 'overflow-key');
                assert.equal(received.length, large.length, `received ${Buffer.isBuffer(received) ? 'buffer' : received}`);
                assert.ok(received.equals(large));
            });
        }
        assert.equal(await master.command('CONFIG', 'SET', 'eviction_policy', 'lru'),
            '-ERR LRU eviction is not supported while daemon replication is active');
        process.kill(daemonPid, 'SIGKILL');
        await eventually(() => assert.equal(masterProcess.exitCode, 1,
            'master must fail explicitly when its capture daemon dies'));
        console.log(`PASS ${backend}: auto-started repld, two replicas, SET/DEL, large payload, fail-fast supervision`);

        for (const socket of sockets) socket.destroy();
        await Promise.all(children.map(stop));
        }

        // A daemon-free master retains the previous RDMA incremental path.
        const legacyMaster = await unusedPort();
        const legacyReplica = await unusedPort();
        const legacyRdma = await unusedPort();
        launch('legacy-master', ['--port', String(legacyMaster), '--replication-port', String(legacyRdma),
            '--replication-ip', address]);
        launch('legacy-replica', ['--port', String(legacyReplica)]);
        const oldMaster = await attach(legacyMaster);
        const oldReplica = await attach(legacyReplica);
        assert.equal(await oldMaster.command('SET', 'before-sync', value), '+OK');
        assert.equal(await oldReplica.command('SLAVEOF', address, legacyRdma), '+OK');
        await eventually(async () => assert.deepEqual(await oldReplica.command('GET', 'before-sync'), value));
        const masterLog = fs.readFileSync(path.join(root, 'legacy-master', 'server.log'), 'utf8');
        assert.match(masterLog, /at offset 0;/,
            'writes before the first replica must not encode or advance the logical log');
        assert.equal(await oldMaster.command('SET', 'legacy-key', value), '+OK');
        await eventually(async () => assert.deepEqual(await oldReplica.command('GET', 'legacy-key'), value));
        assert.equal(await oldMaster.command('DEL', 'before-sync'), ':1');
        await eventually(async () => assert.equal(await oldReplica.command('GET', 'before-sync'), '$-1'));
        assert.equal(await oldMaster.command('CONFIG', 'SET', 'appendonly', 'yes'), '+OK');
        const durable = Buffer.alloc(1024, 42);
        assert.equal(await oldMaster.command('SET', 'aof-key', durable), '+OK');
        await eventually(async () => assert.deepEqual(await oldReplica.command('GET', 'aof-key'), durable));
        assert.equal(await oldMaster.command('CONFIG', 'SET', 'appendonly', 'no'), '+OK');
        assert.equal(await oldMaster.command('SET', 'aof-off', value), '+OK');
        await eventually(async () => assert.deepEqual(await oldReplica.command('GET', 'aof-off'), value));
        assert.deepEqual(fs.readFileSync(path.join(root, 'legacy-master', 'appendonly.aof')),
            encode(['SET', 'aof-key', durable]), 'disk AOF must contain the same canonical bytes as the log hook');
        console.log(`PASS ${backend}: legacy RDMA incremental fallback`);
    } catch (error) {
        for (const child of fs.readdirSync(root)) {
            const log = path.join(root, child, 'server.log');
            if (fs.existsSync(log)) console.error(`${child}:\n${fs.readFileSync(log, 'utf8').slice(-6000)}`);
        }
        throw error;
    } finally {
        for (const socket of sockets) socket.destroy();
        await Promise.all(children.map(stop));
        fs.rmSync(root, { recursive: true });
    }
}

async function connectDaemon(port) {
    return eventually(() => connect(port));
}

main().catch(error => { console.error(error); process.exitCode = 1; });
