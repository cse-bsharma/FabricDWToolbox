/**
 * Fabric DW Query Performance Analyzer - Web App JavaScript
 */

document.addEventListener('DOMContentLoaded', () => {
    // Elements
    const tabs = document.querySelectorAll('.tab');
    const tabContents = document.querySelectorAll('.tab-content');
    const queryForm = document.getElementById('queryForm');
    const xmlForm = document.getElementById('xmlForm');
    const authMethod = document.getElementById('authMethod');
    const credentialsRow = document.getElementById('credentialsRow');
    const uploadArea = document.getElementById('uploadArea');
    const planFileInput = document.getElementById('planFile');
    const planXmlTextarea = document.getElementById('planXml');
    const helpBtn = document.getElementById('helpBtn');
    const helpModal = document.getElementById('helpModal');
    const clearBtn = document.getElementById('clearBtn');
    const exportBtn = document.getElementById('exportBtn');
    
    // State
    let currentResults = null;
    const runBtn = document.getElementById('runBtn');

    // Tab switching
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const targetTab = tab.dataset.tab;
            
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            
            tabContents.forEach(content => {
                content.classList.remove('active');
                if (content.id === `${targetTab}-tab`) {
                    content.classList.add('active');
                }
            });
        });
    });

    // Auth method change
    authMethod.addEventListener('change', () => {
        if (authMethod.value === 'ActiveDirectoryPassword') {
            credentialsRow.style.display = 'grid';
        } else {
            credentialsRow.style.display = 'none';
        }
    });

    // Query form submission
    queryForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        const formData = {
            server: document.getElementById('server').value,
            database: document.getElementById('database').value,
            auth_method: authMethod.value,
            username: document.getElementById('username').value,
            password: document.getElementById('password').value,
            query: document.getElementById('query').value
        };
        
        await analyzeQuery('/analyze', formData);
    });

    // XML form submission
    xmlForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        const planXml = planXmlTextarea.value;
        
        if (!planXml) {
            showError('Please provide an execution plan XML');
            return;
        }
        
        await analyzeQuery('/analyze-xml', { plan_xml: planXml });
    });

    // File upload handling
    uploadArea.addEventListener('click', () => {
        planFileInput.click();
    });

    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('dragover');
    });

    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('dragover');
    });

    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
        
        const file = e.dataTransfer.files[0];
        if (file) {
            handleFileUpload(file);
        }
    });

    planFileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (file) {
            handleFileUpload(file);
        }
    });

    async function handleFileUpload(file) {
        const reader = new FileReader();
        reader.onload = async (e) => {
            planXmlTextarea.value = e.target.result;
            uploadArea.querySelector('p').innerHTML = `Loaded: ${file.name}`;
        };
        reader.readAsText(file);
    }

    // Clear form
    clearBtn.addEventListener('click', () => {
        queryForm.reset();
        credentialsRow.style.display = 'none';
    });

    // Run Query button
    runBtn.addEventListener('click', async () => {
        const formData = {
            server: document.getElementById('server').value,
            database: document.getElementById('database').value,
            auth_method: authMethod.value,
            username: document.getElementById('username').value,
            password: document.getElementById('password').value,
            query: document.getElementById('query').value,
            max_rows: 1000
        };
        
        await runQuery(formData);
    });

    // Export results
    exportBtn.addEventListener('click', () => {
        if (!currentResults) return;
        
        const blob = new Blob([JSON.stringify(currentResults, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `query-analysis-${new Date().toISOString().slice(0, 10)}.json`;
        a.click();
        URL.revokeObjectURL(url);
    });

    // Help modal
    helpBtn.addEventListener('click', () => {
        helpModal.classList.add('active');
    });

    helpModal.addEventListener('click', (e) => {
        if (e.target === helpModal || e.target.classList.contains('modal-close')) {
            helpModal.classList.remove('active');
        }
    });

    // Analysis function
    async function analyzeQuery(endpoint, data) {
        showLoading();
        
        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            
            const result = await response.json();
            
            if (result.success) {
                currentResults = result;
                showResults(result);
                exportBtn.disabled = false;
            } else {
                showError(result.error, result.traceback);
            }
        } catch (error) {
            showError('Failed to connect to analysis server: ' + error.message);
        }
    }

    function showLoading(message = 'Analyzing query...') {
        document.getElementById('emptyState').style.display = 'none';
        document.getElementById('errorState').style.display = 'none';
        document.getElementById('resultsContent').style.display = 'none';
        document.getElementById('queryResultsContent').style.display = 'none';
        document.getElementById('loadingState').style.display = 'flex';
        document.querySelector('#loadingState p').textContent = message;
    }

    function showError(message, trace = null) {
        document.getElementById('emptyState').style.display = 'none';
        document.getElementById('loadingState').style.display = 'none';
        document.getElementById('resultsContent').style.display = 'none';
        document.getElementById('queryResultsContent').style.display = 'none';
        
        document.getElementById('errorMessage').textContent = message;
        
        const detailsEl = document.getElementById('errorDetails');
        const traceEl = document.getElementById('errorTrace');
        
        if (trace) {
            traceEl.textContent = trace;
            detailsEl.style.display = 'block';
        } else {
            detailsEl.style.display = 'none';
        }
        
        document.getElementById('errorState').style.display = 'flex';
    }

    function showResults(results) {
        document.getElementById('emptyState').style.display = 'none';
        document.getElementById('loadingState').style.display = 'none';
        document.getElementById('errorState').style.display = 'none';
        document.getElementById('queryResultsContent').style.display = 'none';
        
        // Summary cards
        document.getElementById('criticalCount').textContent = results.summary.critical;
        document.getElementById('highCount').textContent = results.summary.high;
        document.getElementById('mediumCount').textContent = results.summary.medium;
        document.getElementById('lowCount').textContent = results.summary.low;
        
        // Statement info
        document.getElementById('statementType').textContent = results.statement_type || 'SELECT';
        document.getElementById('estimatedRows').textContent = results.estimated_rows?.toLocaleString() || '-';
        
        // Query History section
        renderQueryHistory(results);
        
        // SQL Pool Info section
        renderSqlPoolInfo(results);
        
        // Statistics info
        const statsSection = document.getElementById('statisticsSection');
        const statsBody = document.getElementById('statisticsTableBody');
        if (results.statistics_info && results.statistics_info.length > 0) {
            statsSection.style.display = 'block';
            statsBody.innerHTML = results.statistics_info.map(s => {
                // Format last_update date
                let lastUpdate = '-';
                if (s.last_update) {
                    try {
                        const d = new Date(s.last_update);
                        lastUpdate = d.toLocaleDateString() + ' ' + d.toLocaleTimeString();
                    } catch (e) {
                        lastUpdate = s.last_update;
                    }
                }
                // Format data type with char length if applicable
                let dataType = s.data_type || '-';
                if (s.char_length != null) {
                    dataType += `(${s.char_length})`;
                }
                return `
                <tr>
                    <td>${s.column_name || '-'}</td>
                    <td>${s.table_name || '-'}</td>
                    <td>${dataType}</td>
                    <td>${lastUpdate}</td>
                    <td>${s.table_cardinality != null ? s.table_cardinality.toLocaleString() : '-'}</td>
                    <td>${s.total_rows != null ? s.total_rows.toLocaleString() : '-'}</td>
                    <td>${s.sampling_percent != null ? s.sampling_percent + '%' : '-'}</td>
                </tr>
            `;
            }).join('');
        } else {
            statsSection.style.display = 'none';
        }
        
        // High-cost data movement operators (Shuffle)
        const highCostSection = document.getElementById('highCostSection');
        const highCostTable = document.getElementById('highCostOperatorsTable');
        if (results.high_cost_operators && results.high_cost_operators.length > 0) {
            highCostSection.style.display = 'block';
            highCostTable.innerHTML = results.high_cost_operators.map(op => `
                <div class="shuffle-operator-card">
                    <div class="shuffle-header">
                        <span class="operator-name">${op.name}</span>
                        <span class="cost-badge">${op.cost_percent}% Cost</span>
                    </div>
                    <div class="shuffle-metrics">
                        <div class="metric-group">
                            <div class="metric">
                                <span class="metric-label">Est. Rows</span>
                                <span class="metric-value">${op.estimated_rows?.toLocaleString() || '-'}</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">Est. CPU</span>
                                <span class="metric-value">${op.estimated_cpu || '-'}</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">Est. I/O</span>
                                <span class="metric-value">${op.estimated_io || '-'}</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">Avg Row Size</span>
                                <span class="metric-value">${op.avg_row_size || '-'} bytes</span>
                            </div>
                        </div>
                    </div>
                    <div class="shuffle-distribution">
                        <div class="distribution-info">
                            <span class="info-label">Distribution Type:</span>
                            <span class="info-value">${op.distribution_type || '-'}${op.move_topology ? ` (${op.move_topology})` : ''}</span>
                        </div>
                        <div class="distribution-info">
                            <span class="info-label">Distribution Key:</span>
                            <span class="info-value distribution-key">${op.distribution_key?.join(', ') || '-'}</span>
                        </div>
                    </div>
                    <div class="shuffle-columns">
                        <span class="info-label">Output Columns:</span>
                        <span class="info-value columns-list" title="${op.output_columns?.join(', ') || '-'}">${op.output_columns?.join(', ') || '-'}</span>
                    </div>
                </div>
            `).join('');
        } else {
            highCostSection.style.display = 'none';
        }
        
        // Sort operators section
        populateCategoryTable('sortOperatorsSection', 'sortOperatorsTableBody', results.sort_operators);
        
        // Recommendations
        const recommendationsList = document.getElementById('recommendationsList');
        recommendationsList.innerHTML = '';
        
        if (results.recommendations.length === 0) {
            recommendationsList.innerHTML = '<p style="color: var(--color-success); text-align: center; padding: 1rem;">No issues found. Query looks good!</p>';
        } else {
            results.recommendations.forEach((rec, index) => {
                const card = document.createElement('div');
                card.className = `recommendation-card ${rec.severity}`;
                card.innerHTML = `
                    <div class="recommendation-header" onclick="this.parentElement.classList.toggle('expanded')">
                        <span class="severity-badge ${rec.severity}">${rec.severity}</span>
                        <span class="recommendation-title">${rec.title}</span>
                        <span class="recommendation-category">${rec.category}</span>
                    </div>
                    <div class="recommendation-body">
                        <p class="recommendation-description">${rec.description}</p>
                        <div class="recommendation-suggestion">
                            <strong>Suggestion:</strong> ${rec.suggestion}
                        </div>
                        ${rec.affected_objects?.length ? `
                            <div class="recommendation-objects">
                                Affected: ${rec.affected_objects.map(obj => `<code>${obj}</code>`).join(' ')}
                            </div>
                        ` : ''}
                    </div>
                `;
                recommendationsList.appendChild(card);
            });
        }
        
        // Joins
        const joinsSection = document.getElementById('joinsSection');
        const joinsList = document.getElementById('joinsList');
        
        if (results.joins && results.joins.length > 0) {
            joinsSection.style.display = 'block';
            joinsList.innerHTML = '';
            
            results.joins.forEach(join => {
                const item = document.createElement('div');
                item.className = 'join-item';
                
                // Format join columns
                const leftCols = join.left_join_columns?.length ? join.left_join_columns.join(', ') : '-';
                const rightCols = join.right_join_columns?.length ? join.right_join_columns.join(', ') : '-';
                
                item.innerHTML = `
                    <span class="join-type">${join.type}</span>
                    <span class="join-tables">${join.tables}</span>
                    <div class="join-columns">
                        <span class="join-col-label">Join Columns:</span>
                        <span class="join-col-value">${leftCols} = ${rightCols}</span>
                    </div>
                    <div class="join-stats">
                        <span>Left: ${join.left_rows?.toLocaleString() || '-'}</span>
                        <span>Right: ${join.right_rows?.toLocaleString() || '-'}</span>
                        <span>Output: ${join.output_rows?.toLocaleString() || '-'}</span>
                        <span>Cost: ${join.cost || '-'}</span>
                        <span>CPU: ${join.cpu || '-'}</span>
                    </div>
                    <div class="join-warning">
                        ${join.is_many_to_many ? '<span class="join-badge many-to-many">Many-to-Many</span>' : ''}
                        ${join.has_type_mismatch ? '<span class="join-badge type-mismatch">Type Mismatch</span>' : ''}
                    </div>
                `;
                joinsList.appendChild(item);
            });
        } else {
            joinsSection.style.display = 'none';
        }
        
        document.getElementById('resultsContent').style.display = 'block';
    }

    // Helper function to populate category tables (join, aggregate, shuffle, sort)
    function populateCategoryTable(sectionId, bodyId, operators) {
        const section = document.getElementById(sectionId);
        const tbody = document.getElementById(bodyId);
        
        if (operators && operators.length > 0) {
            section.style.display = 'block';
            tbody.innerHTML = operators.map(op => `
                <tr>
                    <td>${op.name}</td>
                    <td>${op.logical_op}</td>
                    <td>${op.estimated_rows?.toLocaleString() || '-'}</td>
                    <td>${op.estimated_cpu || '-'}</td>
                    <td><span class="cost-badge ${op.cost_percent > 20 ? 'high' : ''}">${op.cost_percent}%</span></td>
                </tr>
            `).join('');
        } else {
            section.style.display = 'none';
        }
    }

    // Run Query function
    async function runQuery(data) {
        showLoading('Executing query...');
        
        try {
            const response = await fetch('/run', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            
            const result = await response.json();
            
            if (result.success) {
                currentResults = result;
                showQueryResults(result);
                exportBtn.disabled = false;
            } else {
                showError(result.error, result.traceback);
            }
        } catch (error) {
            showError('Failed to execute query: ' + error.message);
        }
    }

    function showQueryResults(results) {
        document.getElementById('emptyState').style.display = 'none';
        document.getElementById('loadingState').style.display = 'none';
        document.getElementById('errorState').style.display = 'none';
        document.getElementById('resultsContent').style.display = 'none';
        
        // Update row count and execution time
        document.getElementById('rowCount').textContent = results.row_count.toLocaleString();
        document.getElementById('executionTime').textContent = `${results.execution_time}s`;
        
        // Build table header
        const thead = document.getElementById('resultsTableHead');
        thead.innerHTML = '<tr>' + results.columns.map(col => `<th>${col}</th>`).join('') + '</tr>';
        
        // Build table body
        const tbody = document.getElementById('resultsTableBody');
        tbody.innerHTML = results.rows.map(row => 
            '<tr>' + row.map(cell => `<td>${cell !== null ? cell : '<em>NULL</em>'}</td>`).join('') + '</tr>'
        ).join('');
        
        // Show/hide "has more" notice
        const hasMoreNotice = document.getElementById('hasMoreNotice');
        if (results.has_more) {
            document.getElementById('maxRowsShown').textContent = results.max_rows;
            hasMoreNotice.style.display = 'block';
        } else {
            hasMoreNotice.style.display = 'none';
        }
        
        document.getElementById('queryResultsContent').style.display = 'block';
    }
    
    // Query History Chart instance
    let executionTimeChart = null;
    
    function renderQueryHistory(results) {
        const section = document.getElementById('queryHistorySection');
        const hashBadge = document.getElementById('queryHashBadge');
        const statsBody = document.getElementById('historyStatsBody');
        
        console.log('Query hash:', results.query_hash);
        console.log('Query history:', results.query_history);
        
        // Hide section if no history or XML-only mode
        if (results.is_xml_only || !results.query_history || 
            (!results.query_history.time_series?.length && !results.query_history.aggregates)) {
            console.log('Hiding query history section - no data');
            section.style.display = 'none';
            return;
        }
        
        console.log('Showing query history section');
        section.style.display = 'block';
        
        // Display query hash
        if (results.query_hash) {
            hashBadge.textContent = results.query_hash;
        }
        
        const history = results.query_history;
        
        // Render the execution time chart
        if (history.time_series && history.time_series.length > 0) {
            renderExecutionTimeChart(history.time_series);
        }
        
        // Render aggregate statistics table
        if (history.aggregates) {
            const agg = history.aggregates;
            
            statsBody.innerHTML = `
                <tr>
                    <td colspan="4" style="text-align: left; background: var(--color-bg);">
                        <span class="count-label">Total Executions:</span>
                        <span class="count-value" style="color: var(--color-primary); font-weight: 600;">${agg.execution_count}</span>
                        &nbsp;&nbsp;|&nbsp;&nbsp;
                        <span class="count-label">Cache Hits:</span>
                        <span class="${agg.cache_hit_percent > 50 ? 'cache-hit-indicator high' : 'cache-hit-indicator low'}">${agg.cache_hit_count} (${agg.cache_hit_percent}%)</span>
                    </td>
                </tr>
                <tr>
                    <td>Elapsed Time (sec)</td>
                    <td>${agg.min_elapsed_seconds}</td>
                    <td>${agg.max_elapsed_seconds}</td>
                    <td>${agg.avg_elapsed_seconds}</td>
                </tr>
                <tr>
                    <td>Data Scanned Remote (MB)</td>
                    <td>${agg.min_data_scanned_remote_mb}</td>
                    <td>${agg.max_data_scanned_remote_mb}</td>
                    <td>${agg.avg_data_scanned_remote_mb}</td>
                </tr>
                <tr>
                    <td>Data Scanned Disk (MB)</td>
                    <td>${agg.min_data_scanned_disk_mb}</td>
                    <td>${agg.max_data_scanned_disk_mb}</td>
                    <td>${agg.avg_data_scanned_disk_mb}</td>
                </tr>
                <tr>
                    <td>Row Count</td>
                    <td>${agg.min_row_count.toLocaleString()}</td>
                    <td>${agg.max_row_count.toLocaleString()}</td>
                    <td>${Math.round(agg.avg_row_count).toLocaleString()}</td>
                </tr>
            `;
        }
    }
    
    function renderExecutionTimeChart(timeSeries) {
        const ctx = document.getElementById('executionTimeChart');
        
        // Destroy existing chart if any
        if (executionTimeChart) {
            executionTimeChart.destroy();
        }
        
        // Reverse to show oldest first (left to right)
        const data = [...timeSeries].reverse();
        
        // Prepare labels (submit_time) and values (elapsed_seconds)
        const labels = data.map(d => {
            if (d.submit_time) {
                const date = new Date(d.submit_time);
                return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
            }
            return '-';
        });
        
        const elapsedData = data.map(d => d.elapsed_seconds || 0);
        const cacheStatus = data.map(d => d.result_cache_hit); // 0=N/A, 1=Created, 2=Hit
        
        // Set point colors based on cache status: 2=Hit (green), 1=Created (yellow), 0=N/A (blue)
        const pointColors = cacheStatus.map(status => {
            if (status === 2) return '#3fb950';  // Cache Hit - green
            if (status === 1) return '#d29922';  // Cache Created - yellow
            return '#58a6ff';  // Not applicable - blue
        });
        
        executionTimeChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Elapsed Time (seconds)',
                    data: elapsedData,
                    borderColor: '#58a6ff',
                    backgroundColor: 'rgba(88, 166, 255, 0.1)',
                    fill: true,
                    tension: 0.3,
                    pointBackgroundColor: pointColors,
                    pointBorderColor: pointColors,
                    pointRadius: 5,
                    pointHoverRadius: 7
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                plugins: {
                    legend: {
                        display: true,
                        labels: {
                            color: '#e6edf3',
                            font: { family: "'Inter', sans-serif" }
                        }
                    },
                    tooltip: {
                        backgroundColor: '#1a1f26',
                        titleColor: '#e6edf3',
                        bodyColor: '#8b949e',
                        borderColor: '#2d3640',
                        borderWidth: 1,
                        callbacks: {
                            afterLabel: function(context) {
                                const idx = context.dataIndex;
                                const d = data[idx];
                                let info = [];
                                // Cache status: 2=Hit, 1=Created, 0=N/A
                                if (d.result_cache_hit === 2) {
                                    info.push('Cache: Hit');
                                } else if (d.result_cache_hit === 1) {
                                    info.push('Cache: Created');
                                } else {
                                    info.push('Cache: N/A');
                                }
                                if (d.data_scanned_remote_mb) {
                                    info.push(`Remote Storage: ${d.data_scanned_remote_mb} MB`);
                                }
                                if (d.row_count) {
                                    info.push(`Rows: ${d.row_count.toLocaleString()}`);
                                }
                                return info;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        grid: { color: '#2d3640' },
                        ticks: { 
                            color: '#8b949e',
                            maxRotation: 45,
                            minRotation: 45,
                            font: { size: 10 }
                        }
                    },
                    y: {
                        grid: { color: '#2d3640' },
                        ticks: { color: '#8b949e' },
                        title: {
                            display: true,
                            text: 'Seconds',
                            color: '#8b949e'
                        },
                        beginAtZero: true
                    }
                }
            }
        });
    }
    
    function renderSqlPoolInfo(results) {
        const section = document.getElementById('sqlPoolInfoSection');
        const tableBody = document.getElementById('sqlPoolInfoBody');
        
        // Hide section if no data or XML-only mode
        if (results.is_xml_only || !results.sql_pool_info || results.sql_pool_info.length === 0) {
            section.style.display = 'none';
            return;
        }
        
        section.style.display = 'block';
        const poolInfo = results.sql_pool_info;
        
        // Build table rows from the pool info records
        let rows = '';
        poolInfo.forEach((record, index) => {
            // Format timestamp
            let timestamp = '-';
            if (record.timestamp) {
                try {
                    const d = new Date(record.timestamp);
                    timestamp = d.toLocaleDateString() + ' ' + d.toLocaleTimeString();
                } catch (e) {
                    timestamp = record.timestamp;
                }
            }
            
            // Extract key metrics based on actual sql_pool_insights columns
            const poolName = record.sql_pool_name || '-';
            const maxResourcePct = record.max_resource_percentage ?? '-';
            const optimizedForReads = record.is_optimized_for_reads;
            const workspaceCapacity = record.current_workspace_capacity || '-';
            const underPressure = record.is_pool_under_pressure;
            
            rows += `
                <tr>
                    <td>${poolName}</td>
                    <td>${timestamp}</td>
                    <td>${typeof maxResourcePct === 'number' ? maxResourcePct + '%' : maxResourcePct}</td>
                    <td><span class="badge ${optimizedForReads === 1 ? 'badge-success' : 'badge-warning'}">${optimizedForReads === 1 ? 'Yes' : 'No'}</span></td>
                    <td>${workspaceCapacity}</td>
                    <td><span class="badge ${underPressure === 0 ? 'badge-success' : 'badge-danger'}">${underPressure === 0 ? 'No' : 'Yes'}</span></td>
                </tr>
            `;
        });
        
        tableBody.innerHTML = rows;
    }
});
