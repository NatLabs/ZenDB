import { useState } from 'react';
import { Table, Flex, Radio, Typography } from 'antd';
import { Buffer } from 'buffer';
import { useUpdateCall } from '@ic-reactor/react';
import { Block } from '../declarations/backend/backend.did';
import { backend, useQueryCall } from '../backend-actor';
import { render } from '@testing-library/react';
import {
    Tag,
    Cascader,
    Input,
    Select,
    Space,
    DatePicker,
    Card,
    Button,
} from 'antd';
import { useQuery } from 'react-query';
import type { SelectProps } from 'antd';

const { RangePicker } = DatePicker;
type TagRender = SelectProps['tagRender'];

const options: SelectProps['options'] = [
    { value: 'gold', label: 'mint' },
    { value: 'red', label: 'burn' },
    { value: 'blue', label: 'transfer' },
    { value: 'green', label: 'approve' },
    { value: 'yellow', label: 'approved-transfers' },
];

const tagRender: TagRender = (props) => {
    const { label, value, closable, onClose } = props;
    const onPreventMouseDown = (event: React.MouseEvent<HTMLSpanElement>) => {
        event.preventDefault();
        event.stopPropagation();
    };
    return (
        <Tag
            color={'blue'}
            onMouseDown={onPreventMouseDown}
            closable={closable}
            onClose={onClose}
            style={{ marginInlineEnd: 4 }}
        >
            {label}
        </Tag>
    );
};

const { Paragraph, Text } = Typography;

const dataSource = [
    {
        key: '1',
        name: 'Mike',
        age: 32,
        address: '10 Downing Street',
    },
    {
        key: '2',
        name: 'John',
        age: 42,
        address: '10 Downing Street',
    },
];

const render_address = (from: Uint8Array | number[]) => {
    return from ? (
        <Text copyable ellipsis>
            {Buffer.from(new Uint8Array(from)).toString('hex')}
        </Text>
    ) : (
        <span>----</span>
    );
};

const columns = [
    { title: 'Block Type', dataIndex: 'btype', key: 'btype' },
    {
        title: 'Fee',
        dataIndex: 'fee',
        key: 'fee',
        render: (fee: bigint) => fee?.toString(),
    },
    {
        title: 'Amount',
        dataIndex: 'amt',
        key: 'amt',
        render: (amt: bigint) =>
            (amt ? Number(amt / 10n ** 8n).toFixed(2) : 0) + ' ICP',
        sorter: () => 0,
    },
    {
        title: 'Time',
        dataIndex: 'ts',
        key: 'ts',
        render: (ts: bigint) =>
            new Date(Number(ts / (1000n * 1000n))).toLocaleString(),
    },
    {
        title: 'Sender',
        dataIndex: 'from',
        key: 'from',
        render: render_address,
    },
    {
        title: 'Recipient',
        dataIndex: 'to',
        key: 'to',
        render: render_address,
    },
    {
        title: 'Spender',
        dataIndex: 'spender',
        key: 'spender',
        render: render_address,
    },
];

type DisplayedTx = {
    ts: bigint;
    to?: Uint8Array;
    amt?: bigint;
    from?: Uint8Array;
    memo?: Uint8Array;
    expected_allowance?: bigint;
    expires_at?: bigint;
    spender?: Uint8Array;
    fee?: bigint;
    btype: string;
    phash?: Blob;
};

export const TxsTable = () => {
    const [txs_query, set_txs_query] = useState({});
    const [txs_query_results, set_txs_query_results] = useState<DisplayedTx[]>(
        [],
    );
    const { data: blocks } = useQuery('txs', async () => {
        const blocks = await backend.get_txs({
            filter: {
                btype: [['1mint']],
                to: [],
                from: [],
                spender: [],
                amt: [],
            },
        });

        const displayed_txs: [DisplayedTx] = blocks.map((block) => ({
            amt: block.tx.amt[0],
            to: block.tx.to[0],
            from: block.tx.from[0],
            memo: block.tx.memo[0],
            expected_allowance: block.tx.expected_allowance[0],
            expires_at: block.tx.expires_at[0],
            spender: block.tx.spender[0],
            fee: block.fee,
            ts: block.ts,
            btype: block.btype,
            phash: block.phash[0],
        })) as any as [DisplayedTx];

        console.log({ displayed_txs });

        return displayed_txs;
    });

    return (
        <Space size="large" direction="vertical">
            <Typography.Title level={2}>
                ZenDB test - Indexing ICP transaction
            </Typography.Title>
            <Space>
                <Card
                    title="Filter Options"
                    style={{ width: 600 }}
                    actions={[<Button>Filter</Button>]}
                >
                    <Space direction="vertical">
                        <Input
                            addonBefore="Recipient"
                            placeholder="Enter recipient address"
                        />
                        <Input
                            addonBefore="Sender"
                            placeholder="Enter sender address"
                        />
                        <Text>Amount</Text>
                        <Flex>
                            <Input addonBefore="Min" placeholder="" />
                            <Input addonBefore="Max" placeholder="" />
                        </Flex>
                        <Typography.Text>Block Type</Typography.Text>
                        <Select
                            mode="multiple"
                            tagRender={tagRender}
                            defaultValue={options.map((o) => o.value)}
                            style={{ width: '100%' }}
                            options={options}
                        />
                        <Space size={'small'} direction="vertical">
                            <Typography.Text>Date/Time</Typography.Text>
                            <RangePicker showTime />
                        </Space>
                    </Space>
                </Card>

                <Card
                    title="Sort Options"
                    style={{ width: 300 }}
                    actions={[<Button>Sort</Button>]}
                >
                    <Space>
                        <Select defaultValue="time" style={{ width: 120 }}>
                            <Select.Option value="time">Time</Select.Option>
                            <Select.Option value="amount">Amount</Select.Option>
                        </Select>
                        <Radio.Group defaultValue="desc">
                            <Radio value="asc">Ascending</Radio>
                            <Radio value="desc">Descending</Radio>
                        </Radio.Group>
                    </Space>
                </Card>
            </Space>

            <Space direction="vertical" style={{ width: '100%' }}>
                <Typography.Title level={3}>Transactions</Typography.Title>
                <Table dataSource={blocks} columns={columns} />
            </Space>
        </Space>
    );
};
