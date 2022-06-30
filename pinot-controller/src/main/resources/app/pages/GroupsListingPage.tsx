/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, {useState, useEffect} from 'react';
import { Grid, makeStyles } from '@material-ui/core';
import {TableData, TableGroup} from 'Models';
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomizedTables from '../components/Table';
import CustomButton from '../components/CustomButton';
import SimpleAccordion from '../components/SimpleAccordion';
import AddSchemaOp from '../components/Homepage/Operations/AddSchemaOp';
import AddOfflineTableOp from '../components/Homepage/Operations/AddOfflineTableOp';
import AddRealtimeTableOp from '../components/Homepage/Operations/AddRealtimeTableOp';

const useStyles = makeStyles(() => ({
    gridContainer: {
        padding: 20,
        backgroundColor: 'white',
        maxHeight: 'calc(100vh - 70px)',
        overflowY: 'auto'
    },
    operationDiv: {
        border: '1px #BDCCD9 solid',
        borderRadius: 4,
        marginBottom: 20
    }
}));

const GroupsListingPage = () => {
    const classes = useStyles();

    const [fetching, setFetching] = useState(true);
    const [groupDetails,setGroupDetails] = useState<Array<string>>([]);
    const [showAddGroupModal, setShowAddGroupModal] = useState(false);
    const groupList = {
        columns: ['Group Name'],
        records: [],
    }
    groupDetails != null && groupDetails.map(x => {
        groupList.records.push([x])
    })

    const fetchData = async () => {
        !fetching && setFetching(true);
        console.log("fetching data")
        const tablesResponse = await PinotMethodUtils.getTableGroupListData();
        console.log("received table response")
        setGroupDetails(tablesResponse)
        setFetching(false);
    };

    useEffect(() => {
        fetchData();
    }, []);

    return fetching ? (
        <AppLoader />
    ) : (
        <Grid item xs className={classes.gridContainer}>
            {/* <div className={classes.operationDiv}>
                <SimpleAccordion
                    headerTitle="Operations"
                    showSearchBox={false}
                >
                    <div>
                        <CustomButton
                            onClick={()=>{setShowAddGroupModal(true)}}
                            tooltipTitle="Create a Pinot table to ingest from batch data sources, such as S3"
                            enableTooltip={true}
                        >
                            Add Table Group
                        </CustomButton>
                    </div>
                </SimpleAccordion>
            </div>*/}
            <CustomizedTables
                title="Groups"
                data={groupList}
                addLinks={true}
                isPagination={true}
                baseURL="/groups/"
                showSearchBox={true}
                inAccordionFormat={true}
            />
            {/*
                showAddGroupModal &&
                <AddOfflineTableOp
                    hideModal={()=>{setShowAddGroupModal(false);}}
                    fetchData={fetchData}
                    tableType={"OFFLINE"}
                />
            */}
        </Grid>
    );
};

export default GroupsListingPage;