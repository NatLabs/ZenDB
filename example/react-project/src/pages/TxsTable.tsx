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
    Collapse,
} from 'antd';
import { useQuery } from 'react-query';
import type { SelectProps } from 'antd';
import { queryClient } from '../utils/react-query-client';
import { useSearch, useLocation } from 'wouter';
import dayjs from 'dayjs';

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

const format_with_icp_decimals = (amt: bigint) => Number(amt) / 10 ** 8;

const render_icp = (amt: bigint) =>
    number_with_comma(
        (amt ? format_with_icp_decimals(BigInt(amt)) : 0).toFixed(8),
    ) + ' ICP';

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
            dayjs(Number(ts / (1000n * 1000n))).format(
                'YYYY-MM-DD, hh:mm:ss A',
            ),
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
    const [performance_state, set_performance_state] = useState({
        instructions: 0,
        time: 0,
    });

    const search_string = useSearch();
    const search_params = new URLSearchParams(search_string);
    const [use_update_method, set_use_update_method] = useState(
        search_params.get('use_update_method') === 'true',
    );

    let options_json_parser_helper = (key: string, value: string) => {
        return typeof value === 'string' && value.match(/^\d+$/)
            ? BigInt(value)
            : value;
    };

    let query = JSON.parse(
        search_params.get('query') || '{}',
        options_json_parser_helper,
    );

    console.log({ query });

    let opt_hex_to_uint8_array = (opt_hex: [] | string[]) =>
        opt_hex.length ? [new Uint8Array(Buffer.from(opt_hex[0], 'hex'))] : [];

    let empty_query: Options = {
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
        count: true,
    };

    const [txs_query, set_txs_query] = useState<Options>(
        'count' in query
            ? {
                  ...query,
                  filter: {
                      ...query?.filter,
                      to: opt_hex_to_uint8_array(query?.filter?.to),
                      from: opt_hex_to_uint8_array(query?.filter?.from),
                      spender: opt_hex_to_uint8_array(query?.filter?.spender),
                  },
              }
            : empty_query,
    );

    const [use_count, set_use_count] = useState(query.count || true);
    const [sorting, set_sorting] = useState(
        query.sort?.length
            ? {
                  field: query.sort[0][0],
                  direction:
                      'Ascending' in query.sort[0][1]
                          ? 'Ascending'
                          : 'Descending',
                  enabled: true,
              }
            : {
                  field: 'ts',
                  direction: 'Ascending' as 'Ascending' | 'Descending',
                  enabled: false,
              },
    );

    const [pagination, set_pagination] = useState(
        query?.pagination
            ? {
                  current: Number(
                      query.pagination.offset / query.pagination.limit,
                  ),
                  page_size: Number(query.pagination.limit || 10n),
              }
            : {
                  current: 1,
                  page_size: 10,
                  total: 0,
              },
    );

    const [location, set_location] = useLocation();

    const {
        data: blocks,
        refetch: refetch_get_txs,
        isFetching: is_table_loading,
        isError: is_table_request_failed,
        error: table_request_error,
    } = useQuery(
        ['get_txs', search_string],
        async () => {
            let query = txs_query;

            console.log(query);
            const start_time = performance.now();

            const res = await (use_update_method
                ? backend.get_async_txs(query)
                : backend.get_txs(query));

            const end_time = performance.now();

            set_pagination((prev) => {
                if (res.total.length) {
                    prev.total = Number(res.total);
                } else {
                    prev.total = undefined;
                }

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
                fee: block.fee[0],
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

        let query = txs_query;

        if (sorting.enabled) {
            query.sort = [
                [
                    sorting.field,
                    sorting.direction === 'Ascending'
                        ? { Ascending: null }
                        : { Descending: null },
                ],
            ];
        } else {
            query.sort = [];
        }

        query.count = use_count;

        set_location(
            `?use_update_method=${use_update_method}&query=${JSON.stringify(
                txs_query,
                (key, value) =>
                    typeof value === 'bigint'
                        ? value.toString()
                        : value instanceof Uint8Array
                        ? Buffer.from(value).toString('hex')
                        : value, // return everything else unchanged
            )
                .replace(' ', '')
                .replace('\n', '')}`,
        );

        // await refetch_get_txs();
    };

    const handle_address_change =
        (field: 'to' | 'from' | 'spender') =>
        (e: React.ChangeEvent<HTMLInputElement>) => {
            const address: string = e?.target?.value || '';

            const uint8_array = new Uint8Array(Buffer.from(address, 'hex'));

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
        (field: 'min' | 'max') => (e: React.ChangeEvent<HTMLInputElement>) => {
            let value = e?.target?.value;
            let num = value === '' ? NaN : Number(value);

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
        set_sorting((prev) => ({
            ...prev,
            field: value,
        }));
    };

    const handle_sort_direction_change = (e: any) => {
        let value = e.target.value;

        set_sorting((prev) => {
            prev.direction = value;
            return prev;
        });
    };

    const number_formatter = Intl.NumberFormat('en', { notation: 'compact' });

    const format_bytes = (bytes: number | bigint) => {
        let formatted = number_formatter
            .format(bytes)
            .replace('B', 'G') // -> replace Billion with Giga
            .replace('T', 'P'); // -> replace Trillion with Peta

        return formatted + 'B';
    };

    let heap_size = format_bytes(stats?.heap_size || 0);
    let stable_memory_size = format_bytes(stats?.stable_memory_size || 0);
    let total_records = number_formatter.format(stats?.db_stats.documents || 0);

    return (
        <Space size="large" direction="vertical">
            <Typography.Title level={2}>
                ZenDB test - Indexing ICP transaction
            </Typography.Title>
            <Collapse
                style={{ padding: 0 }}
                items={[
                    {
                        key: '1',
                        label: (
                            <Flex justify="space-between">
                                <Typography.Title
                                    level={5}
                                    style={{ margin: 0 }}
                                >
                                    Txs Collection Stats
                                </Typography.Title>

                                <Flex wrap={'wrap'} gap={15}>
                                    <Typography.Text>
                                        Heap Size: {heap_size}{' '}
                                    </Typography.Text>
                                    <Typography.Text>
                                        Stable Memory: {stable_memory_size}{' '}
                                    </Typography.Text>
                                    <Typography.Text>
                                        Total Records: {total_records}
                                    </Typography.Text>
                                </Flex>
                            </Flex>
                        ),
                        children: (
                            <Card
                                style={{ margin: 0 }}
                                bordered={false}
                                actions={[
                                    <Button
                                        onClick={async (e) =>
                                            await refetch_get_stats()
                                        }
                                    >
                                        Fetch Stats
                                    </Button>,
                                ]}
                            >
                                <Space direction="vertical">
                                    <Typography.Text>
                                        A summary of txs collection's memory
                                        usage.
                                    </Typography.Text>

                                    <Typography.Paragraph>
                                        Each card displays the indexed fields
                                        and memory usage of each index in the
                                        collection. The indexes use a stable
                                        memory B-tree to store their index
                                        fields and pointers to the documents. The
                                        B-tree has some memory overhead for
                                        storing metadata (internal nodes, leaf
                                        nodes, and pointers), while the rest is
                                        index data. The main index stores the
                                        documents and a default ':id' created by
                                        the system. The other B-trees are
                                        user-created indexes to speed up
                                        queries. Each index is created on a
                                        specific set of fields in a specific
                                        order.
                                    </Typography.Paragraph>

                                    <Typography.Paragraph>
                                        The following indexes were chosen as the
                                        best fit for the queries on this
                                        collection. The ':id' field is
                                        automatically added by the system to all
                                        indexes to support indexing duplicate
                                        values (documents with the same value for
                                        the indexed field). The fields 'ts' and
                                        'tx.amt' are included in most indexes to
                                        minimize the need for in-memory sorting,
                                        which is quite expensive. This, however,
                                        must be manually added by the user.
                                    </Typography.Paragraph>

                                    <Flex gap={5} wrap={'wrap'}>
                                        <Card
                                            size="small"
                                            title="main index: [:id]"
                                            style={{ minWidth: 300 }}
                                            hoverable
                                        >
                                            <Space direction="vertical">
                                                <Typography.Text>
                                                    B-tree Metadata:{' '}
                                                    {format_bytes(
                                                        stats?.db_stats
                                                            ?.main_btree_index
                                                            .stable_memory
                                                            .metadata_bytes ||
                                                            0,
                                                    )}
                                                </Typography.Text>

                                                <Typography.Text>
                                                    Index Data Size:{' '}
                                                    {format_bytes(
                                                        stats?.db_stats
                                                            ?.main_btree_index
                                                            .stable_memory
                                                            .actual_data_bytes ||
                                                            0,
                                                    )}
                                                </Typography.Text>
                                            </Space>
                                        </Card>

                                        {stats?.db_stats?.indexes.map(
                                            (index, i) => (
                                                <Card
                                                    size="small"
                                                    key={i}
                                                    title={
                                                        'index: [' +
                                                        index.columns.join(
                                                            ', ',
                                                        ) +
                                                        ']'
                                                    }
                                                    style={{ minWidth: 300 }}
                                                >
                                                    <Space direction="vertical">
                                                        <Typography.Text>
                                                            B-tree Metadata:{' '}
                                                            {format_bytes(
                                                                index
                                                                    .stable_memory
                                                                    .metadata_bytes,
                                                            )}
                                                        </Typography.Text>

                                                        <Typography.Text>
                                                            Index Data Size:{' '}
                                                            {format_bytes(
                                                                index
                                                                    .stable_memory
                                                                    .actual_data_bytes,
                                                            )}
                                                        </Typography.Text>
                                                    </Space>
                                                </Card>
                                            ),
                                        )}
                                    </Flex>
                                </Space>
                            </Card>
                        ),
                    },
                ]}
            />
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
                <Flex>
                    <Card title="Filter Options" style={{ minWidth: 600 }}>
                        <Space direction="vertical">
                            <Typography.Text> Addresses </Typography.Text>
                            <Input
                                allowClear={true}
                                addonBefore="Sender"
                                placeholder="Enter sender address"
                                defaultValue={query?.filter?.from?.[0] || ''}
                                onChange={handle_address_change('from')}
                            />
                            <Input
                                allowClear={true}
                                addonBefore="Recipient"
                                placeholder="Enter recipient address"
                                defaultValue={query?.filter?.to?.[0] || ''}
                                onChange={handle_address_change('to')}
                            />

                            <Text>Amount</Text>
                            <Flex gap={10} style={{ width: '100%' }}>
                                <Input
                                    allowClear={true}
                                    addonBefore="Min"
                                    placeholder="Enter Min Amount"
                                    defaultValue={
                                        query?.filter?.amt?.[0]?.min?.[0]
                                            ? format_with_icp_decimals(
                                                  BigInt(
                                                      query?.filter?.amt?.[0]
                                                          ?.min?.[0],
                                                  ) || 0n,
                                              ).toFixed(0)
                                            : ''
                                    }
                                    onChange={handle_amt_filter_change('min')}
                                />
                                <Input
                                    allowClear={true}
                                    addonBefore="Max"
                                    placeholder="Enter Max Amount"
                                    defaultValue={
                                        query?.filter?.amt?.[0]?.max?.[0]
                                            ? format_with_icp_decimals(
                                                  BigInt(
                                                      query?.filter?.amt?.[0]
                                                          ?.max?.[0],
                                                  ) || 0n,
                                              ).toFixed(0)
                                            : ''
                                    }
                                    onChange={handle_amt_filter_change('max')}
                                />
                            </Flex>
                            <Typography.Text>Block Type</Typography.Text>
                            <Select
                                allowClear={true}
                                mode="multiple"
                                tagRender={tagRender}
                                defaultValue={
                                    txs_query?.filter?.btype?.[0] || []
                                }
                                style={{ width: '100%' }}
                                options={block_types}
                                onChange={select_block_types}
                            />

                            <Typography.Text>Date/Time</Typography.Text>
                            <RangePicker
                                showTime
                                onChange={handle_date_change}
                                defaultValue={
                                    query?.filter?.ts?.[0]?.min?.[0] &&
                                    query?.filter?.ts?.[0]?.max?.[0]
                                        ? [
                                              dayjs(
                                                  new Date(
                                                      Number(
                                                          query?.filter?.ts?.[0]
                                                              ?.min?.[0],
                                                      ) / 1000000,
                                                  ),
                                              ),
                                              dayjs(
                                                  new Date(
                                                      Number(
                                                          query?.filter?.ts?.[0]
                                                              ?.max?.[0],
                                                      ) / 1000000,
                                                  ),
                                              ),
                                          ]
                                        : [null, null]
                                }
                            />
                        </Space>
                    </Card>

                    <Flex wrap>
                        <Card
                            // title="Sort Options"

                            title={
                                <Flex justify="space-between">
                                    <Typography.Title
                                        level={5}
                                        style={{ margin: 0 }}
                                    >
                                        Sorting
                                    </Typography.Title>
                                    <Switch
                                        checkedChildren="Enabled"
                                        unCheckedChildren="Disabled"
                                        defaultChecked={
                                            query.sort?.length ? true : false
                                        }
                                        onChange={(checked) => {
                                            set_sorting((prev) => ({
                                                ...prev,
                                                enabled: checked,
                                            }));
                                        }}
                                    />
                                </Flex>
                            }
                        >
                            <Space>
                                <Select
                                    style={{ width: 120 }}
                                    defaultValue={query.sort?.[0]?.[0] || 'ts'}
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
                                    defaultValue="Ascending"
                                    onChange={handle_sort_direction_change}
                                >
                                    <Radio value="Ascending">Ascending</Radio>
                                    <Radio value="Descending">Descending</Radio>
                                </Radio.Group>
                            </Space>
                        </Card>
                        <Card
                            title={
                                <Flex justify="space-between">
                                    <Typography.Title
                                        level={5}
                                        style={{ margin: 0 }}
                                    >
                                        Count Query
                                    </Typography.Title>
                                    <Switch
                                        checkedChildren="Enabled"
                                        unCheckedChildren="Disabled"
                                        value={use_count}
                                        onChange={(checked) =>
                                            set_use_count(checked)
                                        }
                                    />
                                </Flex>
                            }
                        >
                            <Space direction="vertical">
                                <Typography.Paragraph>
                                    We perform two database queries: one for the
                                    paginated data and another for the total
                                    document count matching the query. Counting
                                    all documents can be resource-intensive if the
                                    query isn't fully covered by a single index,
                                    as it may require additional in-memory
                                    operations like sorting, filtering, or
                                    merging results from multiple indexes.
                                    Disabling the count query allows fetching
                                    data from page 1 up to the page where it
                                    exceeds the instruction limit.
                                </Typography.Paragraph>
                            </Space>
                        </Card>

                        <Card
                            title={
                                <Flex justify="space-between">
                                    <Typography.Title
                                        level={5}
                                        style={{ margin: 0 }}
                                    >
                                        Use update method
                                    </Typography.Title>
                                    <Switch
                                        onChange={(checked) => {
                                            set_use_update_method(checked);
                                        }}
                                        value={use_update_method}
                                        checkedChildren="Enabled"
                                        unCheckedChildren="Disabled"
                                    />
                                </Flex>
                            }
                        >
                            <Space direction="vertical">
                                <Typography.Paragraph>
                                    Some queries require more computation than
                                    the 5 billion instructions allowed for a
                                    single query call. You can switch to an
                                    update call, which increases the limit to 40
                                    billion instructions. However, update calls
                                    will be slower than query calls.
                                </Typography.Paragraph>
                            </Space>
                        </Card>
                    </Flex>
                </Flex>
            </Card>

            {is_table_request_failed && (
                <Alert
                    message="Table Request Failed"
                    showIcon
                    description={(table_request_error as any)?.message}
                    type="error"
                    // closable
                />
            )}

            <Space direction="vertical" style={{ width: '100%' }}>
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
                                    {pagination?.total == null ||
                                    pagination?.total == undefined
                                        ? '???'
                                        : number_formatter.format(
                                              Number(pagination.total),
                                          )}
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
                    total={Number(
                        pagination.total || stats?.db_stats.documents || 0,
                    )}
                    pageSize={pagination.page_size}
                    onChange={handle_pagination_change}
                />
            </Space>
        </Space>
    );
};
