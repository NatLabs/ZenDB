import { useMemo, useState } from 'react';
import { Table, Flex, Radio, Typography } from 'antd';
import { Buffer } from 'buffer';
import { useUpdateCall } from '@ic-reactor/react';
import { Block } from '../declarations/backend/backend.did';
import { backend, useQueryCall, Options } from '../backend-actor';
import { render } from '@testing-library/react';
import type { InputNumberProps } from 'antd';
import {
    Tag,
    Cascader,
    Input,
    Select,
    Space,
    DatePicker,
    Card,
    Button,
    InputNumber,
    Pagination,
    Switch,
    Alert,
} from 'antd';
import { useQuery } from 'react-query';
import { chronify } from 'chronify';
import type { SelectProps } from 'antd';

const { RangePicker } = DatePicker;
type TagRender = SelectProps['tagRender'];

const block_types: SelectProps['options'] = [
    { value: '1mint', label: 'mint' },
    { value: '1burn', label: 'burn' },
    { value: '1xfer', label: 'transfer' },
    { value: '2approve', label: 'approve' },
    { value: '2xfer', label: 'approved-transfers' },
];

const default_block_types: string[] = block_types.map(
    (btype) => btype.value as string,
);

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

const number_with_comma = (n: string) => {
    let [whole, decimal] = n.split('.');
    let whole_with_comma = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    return decimal ? whole_with_comma + '.' + decimal : whole_with_comma;
};

const render_address = (from: Uint8Array | number[]) => {
    return from ? (
        <Text copyable ellipsis>
            {Buffer.from(new Uint8Array(from)).toString('hex')}
        </Text>
    ) : (
        <span>----</span>
    );
};

const render_icp = (amt: bigint) =>
    number_with_comma((amt ? Number(BigInt(amt) / 10n ** 8n) : 0).toFixed(8)) +
    ' ICP';

const columns = [
    { title: 'Block Type', dataIndex: 'btype', key: 'btype' },
    {
        title: 'Fee',
        dataIndex: 'fee',
        key: 'fee',
        render: render_icp,
    },
    {
        title: 'Amount',
        dataIndex: 'amt',
        key: 'amt',
        render: render_icp,
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

type TxsQuery = {
    filter: {
        btype?: string;
        to?: Blob;
        from?: Blob;
        spender?: Blob;
        amt?: {
            min?: number;
            max?: number;
        };
    };
};

export const TxsTable = () => {
    const [txs_query, set_txs_query] = useState<Options>({
        filter: {
            btype: [],
            to: [],
            from: [],
            spender: [],
            amt: [],
            ts: [],
        },
        sort: [],
        pagination: {
            limit: 10n,
            offset: 0n,
        },
        count : true
    });

    const [pagination, set_pagination] = useState({
        current: 1,
        page_size: 10,
        total: 0,
    });

    const [enable_sorting, set_enable_sorting] = useState(false);
    const [performance_state, set_performance_state] = useState({
        instructions: 0,
        time: 0,
    });
    const [use_async, set_use_async] = useState(false);

    const {
        data: blocks,
        refetch: refetch_get_txs,
        isFetching: is_table_loading,
        isError: is_table_request_failed,
        error: table_request_error,
    } = useQuery(
        ['get_txs', use_async],
        async () => {
            console.log(txs_query);
            const start_time = performance.now();

            const res = await (use_async
                ? backend.get_async_txs(txs_query)
                : backend.get_txs(txs_query));

            const end_time = performance.now();

            set_pagination((prev) => {
                prev.total = Number(res.total);
                return prev;
            });

            set_performance_state((prev) => {
                prev.instructions = Number(res.instructions);
                prev.time = end_time - start_time;
                return prev;
            });

            const displayed_txs: [DisplayedTx] = res.blocks.map((block) => ({
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

            console.log({
                displayed_txs,
                total: res.total,
                instructions: res.instructions,
            });

            return displayed_txs;
        },
        {
            cacheTime: 0,
        },
    );

    const {
        data: stats,
        refetch: refetch_get_stats,
        isFetching: is_stats_loading,
        isError: is_stats_request_failed,
        error: stats_request_error,
    } = useQuery(
        ['get_stats'],
        async () => {
            const stats = await backend.get_stats();
            console.log({ stats });
            return stats;
        },
        {
            cacheTime: 0,
        },
    );

    const set_current_page = (page: number) => {
        set_pagination((prev) => {
            prev.current = page;
            return prev;
        });
    };

    const handle_pagination_change = async (
        page: number,
        page_size: number,
    ) => {
        set_txs_query((prev) => {
            prev.pagination = {
                limit: BigInt(page_size),
                offset: BigInt(page_size * (page - 1)),
            };
            return prev;
        });
        set_pagination((prev) => {
            prev.current = page;
            prev.page_size = page_size;
            return prev;
        });
        await refetch_get_txs().then(() => {});
    };

    const handle_address_change =
        (field: 'to' | 'from' | 'spender') =>
        (e: React.ChangeEvent<HTMLInputElement>) => {
            console.log({
                e,
                target: e.target,
                currentTarget: e.currentTarget,
                value: e.target.value,
            });

            const address: string = e?.target?.value || '';

            const uint8_array = new Uint8Array(Buffer.from(address, 'hex'));

            console.log({ address, uint8_array });

            set_txs_query((prev) => {
                if (address === '') {
                    prev.filter[field] = [];
                } else {
                    prev.filter[field] = [uint8_array];
                }

                return prev;
            });
        };

    let ICP_LEDGER_DECIMALS = 8;

    const handle_amt_filter_change =
        (field: 'min' | 'max'): InputNumberProps['onChange'] =>
        (value) => {
            let num = Number(value);

            console.log({ field, num });

            set_txs_query((prev) => {
                prev.filter.amt = [
                    {
                        min: [],
                        max: [],
                        ...prev.filter.amt?.[0],
                        [field]: isNaN(num)
                            ? []
                            : [num * 10 ** ICP_LEDGER_DECIMALS],
                    },
                ];
                return prev;
            });
        };

    const select_block_types = (value: string[], options: any) => {
        set_txs_query((prev) => {
            if (value.length === 0) {
                prev.filter.btype = [];
            } else {
                prev.filter.btype = [value];
            }
            return prev;
        });
    };

    const handle_date_change = (dates: any) => {
        console.log({ dates });

        if (dates === null) {
            return set_txs_query((prev) => {
                prev.filter.ts = [];
                return prev;
            });
        }

        const min_ts = BigInt(dates[0].valueOf()) * 1000n ** 2n;
        const max_ts = BigInt(dates[1].valueOf()) * 1000n ** 2n;

        set_txs_query((prev) => {
            prev.filter.ts = [
                {
                    min: [min_ts],
                    max: [max_ts],
                },
            ];
            return prev;
        });
    };

    const handle_sort_field_change = (value: string) => {
        set_txs_query((prev) => {
            if (enable_sorting) {
                if (prev.sort.length === 0) {
                    prev.sort = [[value, { Ascending: null }]];
                } else {
                    prev.sort[0][0] = value;
                }
            }

            return prev;
        });
    };

    const handle_sort_direction_change = (e: any) => {
        let value = e.target.value;

        set_txs_query((prev) => {
            if (enable_sorting) {
                if (prev.sort.length > 0) {
                    prev.sort[0][1] =
                        value == 'Ascending'
                            ? { Ascending: null }
                            : { Descending: null };
                }
            }

            return prev;
        });
    };

    const number_formatter = Intl.NumberFormat('en', { notation: 'compact' });

    const format_bytes = (bytes: number | bigint) => {
        let formatted = number_formatter
            .format(bytes) // too lazy to write a function to format bytes
            .replace('B', 'G') // -> replace Billion with Giga
            .replace('T', 'P'); // -> replace Trillion with Peta

        return formatted + 'B';
    };

    return (
        <Space size="large" direction="vertical">
            <Typography.Title level={2}>
                ZenDB test - Indexing ICP transaction
            </Typography.Title>
            <Card
                actions={[
                    <Button
                        onClick={async (e) => {
                            handle_pagination_change(1, pagination.page_size);
                            // await refetch_get_txs();
                            // set_current_page(1);
                        }}
                    >
                        Filter and Sort
                    </Button>,
                ]}
            >
                <Card
                    title="Txs Collection Stats"
                    actions={[
                        <Button
                            onClick={async (e) => await refetch_get_stats()}
                        >
                            Fetch Stats
                        </Button>,
                    ]}
                >
                    <Space direction="vertical">
                        <Typography.Text>
                            Displays the stats for the transactions collection
                            in zenDB
                        </Typography.Text>
                        <Typography.Text>
                            Heap Size: {format_bytes(stats?.heap_size || 0)}
                        </Typography.Text>
                        <Typography.Text>
                            Stable Memory Size:{' '}
                            {format_bytes(stats?.stable_memory_size || 0)}
                        </Typography.Text>
                        <Typography.Text>
                            Total txs records:{' '}
                            {number_formatter.format(
                                stats?.db_stats.records || 0,
                            )}
                        </Typography.Text>
                    </Space>

                    <Flex gap={5} wrap={'wrap'}>
                        <Card
                            title="Main BTree Stats"
                            style={{ minWidth: 250 }}
                        >
                            <Space direction="vertical">
                                <Typography.Text>
                                    Metadata Bytes:{' '}
                                    {format_bytes(
                                        stats?.db_stats?.main_btree_index
                                            .stable_memory.metadata_bytes || 0,
                                    )}
                                </Typography.Text>

                                <Typography.Text>
                                    Actual Data Bytes:{' '}
                                    {format_bytes(
                                        stats?.db_stats?.main_btree_index
                                            .stable_memory.actual_data_bytes ||
                                            0,
                                    )}
                                </Typography.Text>
                            </Space>
                        </Card>

                        {stats?.db_stats?.indexes.map((index, i) => (
                            <Card
                                key={i}
                                title={
                                    'index: [' + index.columns.join(', ') + ']'
                                }
                                style={{ minWidth: 250 }}
                            >
                                <Space direction="vertical">
                                    <Typography.Text>
                                        Metadata Bytes:{' '}
                                        {format_bytes(
                                            index.stable_memory.metadata_bytes,
                                        )}
                                    </Typography.Text>

                                    <Typography.Text>
                                        Actual Data Bytes:{' '}
                                        {format_bytes(
                                            index.stable_memory
                                                .actual_data_bytes,
                                        )}
                                    </Typography.Text>
                                </Space>
                            </Card>
                        ))}
                    </Flex>
                </Card>
                <Flex>
                    <Card title="Filter Options" style={{ minWidth: 600 }}>
                        <Space direction="vertical">
                            <Typography.Text> Addresses </Typography.Text>
                            <Input
                                allowClear={true}
                                addonBefore="Sender"
                                placeholder="Enter sender address"
                                onChange={handle_address_change('from')}
                            />
                            <Input
                                allowClear={true}
                                addonBefore="Recipient"
                                placeholder="Enter recipient address"
                                onChange={handle_address_change('to')}
                            />

                            <Text>Amount</Text>
                            <Flex>
                                <InputNumber
                                    addonBefore="Min"
                                    placeholder="Enter Min Amount"
                                    onChange={handle_amt_filter_change('min')}
                                />
                                <InputNumber
                                    addonBefore="Max"
                                    placeholder="Enter Max Amount"
                                    onChange={handle_amt_filter_change('max')}
                                />
                            </Flex>
                            <Typography.Text>Block Type</Typography.Text>
                            <Select
                                allowClear={true}
                                mode="multiple"
                                tagRender={tagRender}
                                defaultValue={[]}
                                style={{ width: '100%' }}
                                options={block_types}
                                onChange={select_block_types}
                            />
                            <Space size={'small'} direction="vertical">
                                <Typography.Text>Date/Time</Typography.Text>
                                <RangePicker
                                    showTime
                                    onChange={handle_date_change}
                                />
                            </Space>
                        </Space>
                    </Card>

                    <Flex vertical>
                        <Card
                            title="Sort Options"
                            style={{ width: 300 }}
                            actions={[
                                <Switch
                                    checkedChildren="Disable Sorting"
                                    unCheckedChildren="Enable Sorting"
                                    defaultChecked={false}
                                    onChange={(checked) => {
                                        set_enable_sorting(checked);
                                        if (!checked) {
                                            set_txs_query((prev) => {
                                                prev.sort = [];
                                                return prev;
                                            });
                                        }
                                    }}
                                />,
                            ]}
                        >
                            <Space>
                                <Select
                                    defaultValue="time"
                                    style={{ width: 120 }}
                                    onChange={handle_sort_field_change}
                                >
                                    <Select.Option value="ts">
                                        Time
                                    </Select.Option>
                                    <Select.Option value="tx.amt">
                                        Amount
                                    </Select.Option>
                                </Select>
                                <Radio.Group
                                    defaultValue="desc"
                                    onChange={handle_sort_direction_change}
                                >
                                    <Radio value="Ascending">Ascending</Radio>
                                    <Radio value="Descending">Descending</Radio>
                                </Radio.Group>
                            </Space>
                        </Card>

                        <Card title="Use update method">
                            <Space direction="vertical">
                                <Typography.Text>
                                    Some queries require more computation than
                                    is allowed for a single query call with the
                                    limit of 5B instructions. You can switch to
                                    an update call to fetch the data. This will
                                    allow you to query data with a limit of 40B
                                    instructions. The downside to this
                                    additional computation is that the update
                                    calls are slower than the query calls.
                                </Typography.Text>

                                <Switch
                                    onChange={(checked) => {
                                        set_use_async(checked);
                                    }}
                                    value={use_async}
                                    checkedChildren="Use Async"
                                    unCheckedChildren="Use Sync"
                                />
                            </Space>
                        </Card>
                    </Flex>
                </Flex>
            </Card>

            <Space direction="vertical" style={{ width: '100%' }}>
                {is_table_request_failed && (
                    <Alert
                        message="Table Request Failed"
                        showIcon
                        description={(table_request_error as any)?.message}
                        type="error"
                        // closable
                        action={
                            <Button size="small" danger>
                                Detail
                            </Button>
                        }
                    />
                )}

                <Table
                    title={() => (
                        <Flex justify="space-between" align="center">
                            <Typography.Title level={3} style={{ margin: 0 }}>
                                Transactions
                            </Typography.Title>
                            <span>
                                <Typography.Text>
                                    Query Performance:{' '}
                                </Typography.Text>
                                <Typography.Text strong>
                                    {number_formatter.format(
                                        performance_state.instructions,
                                    )}{' '}
                                    Instructions
                                </Typography.Text>
                                {' | '}
                                <Typography.Text strong>
                                    {performance_state.time / 1000 >= 60
                                        ? (
                                              performance_state.time /
                                              1000 /
                                              60
                                          ).toFixed(0) +
                                          ' min' +
                                          ' ' +
                                          (
                                              (performance_state.time %
                                                  60_000) /
                                              1000
                                          ).toFixed(0) +
                                          ' s'
                                        : (
                                              (performance_state.time %
                                                  60_000) /
                                              1000
                                          ).toFixed(2) + ' s'}
                                </Typography.Text>
                            </span>
                            <span>
                                <Typography.Text>
                                    Total Transactions:{' '}
                                </Typography.Text>
                                <Typography.Text strong>
                                    {number_formatter.format(pagination.total)}
                                </Typography.Text>
                            </span>
                        </Flex>
                    )}
                    dataSource={is_table_request_failed ? [] : blocks}
                    columns={columns}
                    loading={is_table_loading}
                    pagination={false}
                />
                <Pagination
                    showQuickJumper
                    current={pagination.current}
                    total={pagination.total}
                    pageSize={pagination.page_size}
                    onChange={handle_pagination_change}
                />
            </Space>
        </Space>
    );
};
