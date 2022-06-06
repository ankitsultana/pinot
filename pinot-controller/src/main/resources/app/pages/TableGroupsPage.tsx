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

import React, { useState, useEffect, useRef } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { FormControlLabel, Grid, Switch, Tooltip } from '@material-ui/core';
import { RouteComponentProps, useHistory, useLocation } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { TableData } from 'Models';
import _ from 'lodash';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import TableToolbar from '../components/TableToolbar';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import SimpleAccordion from '../components/SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomButton from '../components/CustomButton';
import EditConfigOp from '../components/Homepage/Operations/EditConfigOp';
import ReloadStatusOp from '../components/Homepage/Operations/ReloadStatusOp';
import RebalanceServerTableOp from '../components/Homepage/Operations/RebalanceServerTableOp';
import Confirm from '../components/Confirm';
import { NotificationContext } from '../components/Notification/NotificationContext';
import Utils from '../utils/Utils';
import InfoOutlinedIcon from '@material-ui/icons/InfoOutlined';

const useStyles = makeStyles((theme) => ({
    root: {
        border: '1px #BDCCD9 solid',
        borderRadius: 4,
        marginBottom: '20px',
    },
    highlightBackground: {
        border: '1px #4285f4 solid',
        backgroundColor: 'rgba(66, 133, 244, 0.05)',
        borderRadius: 4,
        marginBottom: '20px',
    },
    body: {
        borderTop: '1px solid #BDCCD9',
        fontSize: '16px',
        lineHeight: '3rem',
        paddingLeft: '15px',
    },
    queryOutput: {
        border: '1px solid #BDCCD9',
        '& .CodeMirror': { height: 532 },
    },
    sqlDiv: {
        border: '1px #BDCCD9 solid',
        borderRadius: 4,
        marginBottom: '20px',
    },
    operationDiv: {
        border: '1px #BDCCD9 solid',
        borderRadius: 4,
        marginBottom: 20
    }
}));

const jsonoptions = {
    lineNumbers: true,
    mode: 'application/json',
    styleActiveLine: true,
    gutters: ['CodeMirror-lint-markers'],
    theme: 'default',
    readOnly: true
};

type Props = {
    groupName: string;
};

type Summary = {
    groupName: string;
    tables: Array<string>;
    config: string;
};

const TableGroupsPage = ({ match }: RouteComponentProps<Props>) => {
    const tableGroupName = match.params.groupName;
    const {dispatch} = React.useContext(NotificationContext);
    const [confirmDialog, setConfirmDialog] = React.useState(false);
    const [dialogDetails, setDialogDetails] = React.useState(null);
    const closeDialog = () => {
        setConfirmDialog(false);
        setDialogDetails(null);
    };
    const [groupSummary, setGroupSummary] = useState<Summary>({
        groupName: tableGroupName,
        tables: [],
        config: "",
   });

    const handleConfigChange = (value: string) => {
        setConfig(value);
    };

    const fetchGroupData = async () => {
        setFetching(true);
        const result = await PinotMethodUtils.getTableGroupData(tableGroupName);
        setGroupSummary(result);
    };
    const syncResponse = (result) => {
        if(result.status){
            dispatch({type: 'success', message: result.status, show: true});
            fetchGroupData();
            setShowEditConfig(false);
        } else {
            dispatch({type: 'error', message: result.error, show: true});
        }
        closeDialog();
    };
    const saveConfigAction = async () => {
        let configObj = JSON.parse(config);
        if(actionType === 'editGroup'){
            if(configObj.OFFLINE || configObj.REALTIME){
                configObj = configObj.OFFLINE || configObj.REALTIME;
            }
            const result = await PinotMethodUtils.updateTable(tableGroupName, configObj);
            syncResponse(result);
        }
    };
    const [fetching, setFetching] = useState(true);
    const [groupConfig, setTableConfig] = useState('');
    const [showEditConfig, setShowEditConfig] = useState(false);
    const [config, setConfig] = useState('{}');
    const [actionType, setActionType] = useState(null);
    const classes = useStyles();
    return fetching ? (
        <AppLoader />
    ) : (
        <Grid
            item
            xs
            style={{
                padding: 20,
                backgroundColor: 'white',
                maxHeight: 'calc(100vh - 70px)',
                overflowY: 'auto',
            }}
        >
            <div className={classes.operationDiv}>
                <SimpleAccordion
                    headerTitle="Operations"
                    showSearchBox={false}
                >
                    <div>
                        <CustomButton
                            onClick={()=>{
                                setActionType('editGroup');
                                setConfig(groupConfig);
                                setShowEditConfig(true);
                            }}
                            tooltipTitle="Edit Table"
                            enableTooltip={true}
                        >
                            Edit Table
                        </CustomButton>
                    </div>
                </SimpleAccordion>
            </div>

            <Grid container spacing={2}>
                <Grid item xs={6}>
                    <div className={classes.sqlDiv}>
                        <SimpleAccordion
                            headerTitle="Table Group Config"
                            showSearchBox={false}
                        >
                            <CodeMirror
                                options={jsonoptions}
                                value={groupConfig}
                                className={classes.queryOutput}
                                autoCursor={false}
                            />
                        </SimpleAccordion>
                    </div>
                </Grid>
            </Grid>
            <EditConfigOp
                showModal={showEditConfig}
                hideModal={()=>{setShowEditConfig(false);}}
                saveConfig={saveConfigAction}
                config={config}
                handleConfigChange={handleConfigChange}
            />
            {confirmDialog && dialogDetails && <Confirm
                openDialog={confirmDialog}
                dialogTitle={dialogDetails.title}
                dialogContent={dialogDetails.content}
                successCallback={dialogDetails.successCb}
                closeDialog={closeDialog}
                dialogYesLabel='Yes'
                dialogNoLabel='No'
            />}
        </Grid>
    );
}

export default TableGroupsPage;
