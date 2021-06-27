import React, {useEffect, useRef, useState} from 'react';
import {VariableSizeGrid as Grid} from 'react-window';
import ResizeObserver from 'rc-resize-observer';
import classNames from 'classnames';
import {Table, Empty, Result} from 'antd';

function VirtualTable(props) {
    const {columns, scroll, className} = props;
    const [tableWidth, setTableWidth] = useState(0);
    const widthColumnCount = columns.filter(({width}) => !width).length;
    const mergedColumns = columns.map(column => {
        if (column.width) {
            return column;
        }

        return {...column, width: Math.floor(tableWidth / widthColumnCount)};
    });
    const gridRef = useRef();
    const [connectObject] = useState(() => {
        const obj = {};
        Object.defineProperty(obj, 'scrollLeft', {
            get: () => null,
            set: scrollLeft => {
                if (gridRef.current) {
                    gridRef.current.scrollTo({
                        scrollLeft,
                    });
                }
            },
        });
        return obj;
    });

    const resetVirtualGrid = () => {
        gridRef.current.resetAfterIndices({
            columnIndex: 0,
            shouldForceUpdate: false,
        });
    };

    useEffect(() => resetVirtualGrid, []);
    useEffect(() => resetVirtualGrid, [tableWidth]);

    const renderVirtualList = (rawData, {scrollbarSize, ref, onScroll}) => {
        ref.current = connectObject;
        return (
            <Grid
                ref={gridRef}
                className="virtual-grid"
                columnCount={mergedColumns.length}
                columnWidth={index => {
                    const {width} = mergedColumns[index];
                    return index === mergedColumns.length - 1 ? width - scrollbarSize - 1 : width;
                }}
                height={scroll.y}
                rowCount={rawData.length}
                rowHeight={() => 54}
                width={tableWidth}
                onScroll={({scrollLeft}) => {
                    onScroll({
                        scrollLeft,
                    });
                }}
            >
                {({columnIndex, rowIndex, style}) => (
                    <div
                        className={classNames('virtual-table-cell', {
                            'virtual-table-cell-last': columnIndex === mergedColumns.length - 1,
                        })}
                        style={style}
                    >
                        {rawData[rowIndex][mergedColumns[columnIndex].dataIndex]}
                    </div>
                )}
            </Grid>
        );
    };

    return (
        <ResizeObserver
            onResize={({width}) => {
                setTableWidth(width);
            }}
        >
            <Table
                {...props}
                className={classNames(className, 'virtual-table')}
                columns={mergedColumns}
                pagination={false}
                components={{
                    body: renderVirtualList,
                }}
            />
        </ResizeObserver>
    );
}


function DataTable(props) {
    const {tableData, tableColumns} = props;
    const getData = (tableData) => {
        return tableData.map(
            (x, idx) => {
                const item = {}
                x.map(
                    (_x, _idx) => {

                        item[_idx] = _x;
                        return _x;
                    }
                )
                return item;
            }
        )
    }


    const _columns = tableColumns.map(
        (x, idx) => {
            return ({
                title: x,
                dataIndex: idx,
                key: x
            })
        }
    )
    const _data = getData(tableData)
    return (
        <VirtualTable
            columns={_columns}
            dataSource={_data}
            scroll={{
                y: 300,
                x: '100vw',
            }}
        />
    )
}

function Callback(props) {
    const {status, times, rows, data} = props
    if (status === true) {
        if (data !== undefined && data !== null && data !== []) {
            const tableColumns = data[0]
            const tableData = data.slice(1)
            return <div style={{
                width: '100%',
                height: '100%'
            }}>
                <Result
                    status="success"
                    title={`操作成功! 本次操作影响了${rows}行数据，耗时${times}s`}
                    style={{
                        backgroundColor: '#FFF'
                    }}
                />
                <DataTable tableData={tableData} tableColumns={tableColumns}/>

            </div>
        }
        return (<div style={{
            width: '100%',
            height: '100%'
        }}>
            <Result
                status="success"
                title={`操作成功!耗时${times}s`}
                style={{
                    backgroundColor: '#FFF'
                }}
            />
        </div>)
    } else if (status === false) {
        return <Result
            status="error"
            title={`操作失败! 后端反馈：${data}`}
            style={{
                backgroundColor: '#FFF'
            }}
        />
    } else if (status === undefined) {
        return <Empty/>
    } else if (times === undefined || times === null || times === []) {
        // TODO 退出登录
        return <Result
            status="success"
            title={'登出成功！'}
            style={{
                backgroundColor: '#FFF'
            }}
        />

    } else {
        return <Result
            status="error"
            title={`啊欧，失败了！可能是你的语句不太正常。`}
            style={{
                backgroundColor: '#FFF'
            }}
        />
    }


}

export default Callback;