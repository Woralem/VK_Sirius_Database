import React, { useState, useEffect } from 'react';
import './App.css';

function App() {
    const [query, setQuery] = useState('');
    const [result, setResult] = useState(null);
    const [error, setError] = useState('');
    const [databases, setDatabases] = useState([]);
    const [selectedDb, setSelectedDb] = useState('default');
    const [newDbName, setNewDbName] = useState('');
    const [showDbManager, setShowDbManager] = useState(false);
    const [renamingDb, setRenamingDb] = useState(null);
    const [renameInput, setRenameInput] = useState('');
    const [showLogs, setShowLogs] = useState(false);
    const [logs, setLogs] = useState([]);
    const [logsLoading, setLogsLoading] = useState(false);

    // Загрузка списка БД при монтировании компонента
    useEffect(() => {
        loadDatabases();
    }, []);

    const loadDatabases = async () => {
        try {
            const response = await fetch('/api/db/list');
            const data = await response.json();
            if (data.status === 'success') {
                setDatabases(data.databases);
            }
        } catch (error) {
            console.error('Failed to load databases:', error);
        }
    };

    const loadLogs = async (limit = 100, offset = 0) => {
        setLogsLoading(true);
        try {
            const response = await fetch(`/api/logs?limit=${limit}&offset=${offset}`);
            const data = await response.json();
            setLogs(data.logs || []);
        } catch (error) {
            console.error('Failed to load logs:', error);
            alert('Failed to load activity logs');
        } finally {
            setLogsLoading(false);
        }
    };

    const downloadLogs = async (format = 'text') => {
        try {
            const response = await fetch(`/api/logs/download?format=${format}`);
            const blob = await response.blob();

            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `activity_log.${format}`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            console.error('Failed to download logs:', error);
            alert('Failed to download logs');
        }
    };

    const clearLogs = async () => {
        if (!window.confirm('Are you sure you want to clear all activity logs?')) {
            return;
        }

        try {
            const response = await fetch('/api/logs/clear', { method: 'POST' });
            const data = await response.json();
            if (data.status === 'success') {
                setLogs([]);
                alert('Logs cleared successfully');
            }
        } catch (error) {
            console.error('Failed to clear logs:', error);
            alert('Failed to clear logs');
        }
    };

    const handleDbChange = async (newDb) => {
        const oldDb = selectedDb;
        setSelectedDb(newDb);

        // Log database switch
        try {
            await fetch('/api/db/switch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    from: oldDb,
                    to: newDb
                })
            });
        } catch (error) {
            console.error('Failed to log database switch:', error);
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setResult(null);

        // Check for special log commands
        const upperQuery = query.toUpperCase().trim();
        if (upperQuery === 'SHOW LOGS' || upperQuery === 'SELECT * FROM LOGS') {
            setShowLogs(true);
            await loadLogs();
            return;
        } else if (upperQuery === 'DOWNLOAD LOGS') {
            await downloadLogs('text');
            return;
        } else if (upperQuery === 'CLEAR LOGS') {
            await clearLogs();
            return;
        }

        const API_URL = '/api/query';

        try {
            const requestBody = JSON.stringify({
                query: query,
                database: selectedDb
            });
            console.log('Sending query to backend:', requestBody);

            const response = await fetch(API_URL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: requestBody,
            });

            const rawResponseText = await response.text();
            console.log('Raw server response text:', rawResponseText);
            console.log('Server response status code:', response.status);

            let data;
            try {
                data = JSON.parse(rawResponseText);
                console.log('Parsed server response data:', data);
            } catch (parseError) {
                console.error('Failed to parse server response as JSON:', parseError);
                setError(`Failed to parse server response. Status: ${response.status}. Raw response: ${rawResponseText.substring(0, 200)}...`);
                return;
            }

            if (data.status === 'success') {
                setResult(data);
                setError('');
            } else {
                let formattedError = data.message || 'An unknown error occurred.';
                if (data.errors && Array.isArray(data.errors)) {
                    formattedError += '\n\nDetails:\n' + data.errors.join('\n');
                }
                setError(formattedError);
                setResult(null);
                console.error('Backend error message:', formattedError, 'Full response data:', data);
            }

        } catch (networkError) {
            console.error("Network or other fetch error:", networkError);
            setError('Failed to connect to the server. Is the C++ backend running on localhost:8080?');
        }
    };

    const createDatabase = async () => {
        if (!newDbName.trim()) return;

        try {
            const response = await fetch('/api/db/create', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ name: newDbName }),
            });

            const data = await response.json();
            if (data.status === 'success') {
                await loadDatabases();
                setNewDbName('');
                setSelectedDb(data.database);
            } else {
                alert(data.message);
            }
        } catch (error) {
            console.error('Failed to create database:', error);
            alert('Failed to create database');
        }
    };

    const deleteDatabase = async (dbName) => {
        if (dbName === 'default') {
            alert("Cannot delete the default database");
            return;
        }

        if (!window.confirm(`Are you sure you want to delete database "${dbName}"?`)) {
            return;
        }

        try {
            const response = await fetch('/api/db/delete', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ name: dbName }),
            });

            const data = await response.json();
            if (data.status === 'success') {
                await loadDatabases();
                if (selectedDb === dbName) {
                    setSelectedDb('default');
                }
            } else {
                alert(data.message);
            }
        } catch (error) {
            console.error('Failed to delete database:', error);
            alert('Failed to delete database');
        }
    };

    const renameDatabase = async (oldName, newName) => {
        if (!newName.trim() || newName === oldName) return;

        try {
            const response = await fetch('/api/db/rename', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ oldName, newName }),
            });

            const data = await response.json();
            if (data.status === 'success') {
                await loadDatabases();
                if (selectedDb === oldName) {
                    setSelectedDb(newName);
                }
                setRenamingDb(null);
                setRenameInput('');
            } else {
                alert(data.message);
            }
        } catch (error) {
            console.error('Failed to rename database:', error);
            alert('Failed to rename database');
        }
    };

    const renderResult = () => {
        if (!result) return null;

        if (result.message && !result.data) {
            return <p><strong>Message:</strong> {result.message}</p>;
        }
        if (typeof result.rows_affected !== 'undefined') {
            return <p><strong>Rows Affected:</strong> {result.rows_affected}</p>
        }

        if (result.data && Array.isArray(result.data)) {
            if (result.data.length === 0) {
                return <p>(Query returned 0 rows)</p>;
            }
            const headers = Object.keys(result.data[0]);
            return (
                <table>
                    <thead>
                    <tr>
                        {headers.map(key => <th key={key}>{key}</th>)}
                    </tr>
                    </thead>
                    <tbody>
                    {result.data.map((row, rowIndex) => (
                        <tr key={rowIndex}>
                            {headers.map(header => (
                                <td key={`${rowIndex}-${header}`}>
                                    {row[header] === null ? <i>NULL</i> : String(row[header])}
                                </td>
                            ))}
                        </tr>
                    ))}
                    </tbody>
                </table>
            );
        }

        return <pre>{JSON.stringify(result, null, 2)}</pre>;
    };

    return (
        <div className="App">
            <button className="logs-button" onClick={() => { setShowLogs(true); loadLogs(); }}>
                📋 Activity Logs
            </button>

            {showLogs && (
                <div className="logs-modal" onClick={() => setShowLogs(false)}>
                    <div className="logs-content" onClick={(e) => e.stopPropagation()}>
                        <div className="logs-header">
                            <h2>Activity Logs</h2>
                            <div className="logs-actions">
                                <button onClick={() => downloadLogs('text')}>Download TXT</button>
                                <button onClick={() => downloadLogs('csv')}>Download CSV</button>
                                <button onClick={() => downloadLogs('json')}>Download JSON</button>
                                <button onClick={clearLogs} className="delete-btn">Clear Logs</button>
                                <button onClick={() => setShowLogs(false)}>Close</button>
                            </div>
                        </div>

                        {logsLoading ? (
                            <p>Loading logs...</p>
                        ) : logs.length === 0 ? (
                            <p>No activity logs found.</p>
                        ) : (
                            <div className="logs-list">
                                {logs.map((log, index) => (
                                    <div key={index} className={`log-entry ${log.success ? 'success' : 'error'}`}>
                                        <div className="log-timestamp">{log.timestamp}</div>
                                        <div className="log-action">{log.action}</div>
                                        <div>Database: {log.database}</div>
                                        {log.query && <div className="log-query">{log.query}</div>}
                                        {log.details && <div>Details: {log.details}</div>}
                                        {log.error && <div style={{color: '#f44336'}}>Error: {log.error}</div>}
                                        {log.result && log.result.data && (
                                            <div className="log-result">
                                                Result: {log.result.truncated ?
                                                `${log.result.data.length} rows (truncated from ${log.result.total_rows})` :
                                                `${log.result.data.length} rows`}
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            )}

            <header className="App-header">
                <h1>Database Query Interface</h1>

                <div className="db-controls">
                    <div className="db-selector">
                        <label>Current Database: </label>
                        <select
                            value={selectedDb}
                            onChange={(e) => handleDbChange(e.target.value)}
                            className="db-select"
                        >
                            {databases.map(db => (
                                <option key={db} value={db}>{db}</option>
                            ))}
                        </select>
                        <button
                            onClick={() => setShowDbManager(!showDbManager)}
                            className="manage-db-btn"
                        >
                            {showDbManager ? 'Hide' : 'Manage'} Databases
                        </button>
                    </div>

                    {showDbManager && (
                        <div className="db-manager">
                            <div className="create-db">
                                <input
                                    type="text"
                                    value={newDbName}
                                    onChange={(e) => setNewDbName(e.target.value)}
                                    placeholder="New database name"
                                    className="db-name-input"
                                />
                                <button onClick={createDatabase}>Create Database</button>
                            </div>
                            <div className="db-list">
                                <h3>Existing Databases:</h3>
                                {databases.map(db => (
                                    <div key={db} className="db-item">
                                        {renamingDb === db ? (
                                            <div className="rename-container">
                                                <input
                                                    type="text"
                                                    value={renameInput}
                                                    onChange={(e) => setRenameInput(e.target.value)}
                                                    placeholder="New name"
                                                    className="rename-input"
                                                    autoFocus
                                                    onKeyPress={(e) => {
                                                        if (e.key === 'Enter') {
                                                            renameDatabase(db, renameInput);
                                                        }
                                                    }}
                                                />
                                                <button
                                                    onClick={() => renameDatabase(db, renameInput)}
                                                    className="save-btn"
                                                >
                                                    Save
                                                </button>
                                                <button
                                                    onClick={() => {
                                                        setRenamingDb(null);
                                                        setRenameInput('');
                                                    }}
                                                    className="cancel-btn"
                                                >
                                                    Cancel
                                                </button>
                                            </div>
                                        ) : (
                                            <>
                                                <span>{db}</span>
                                                <div className="db-actions">
                                                    {db !== 'default' && (
                                                        <>
                                                            <button
                                                                onClick={() => {
                                                                    setRenamingDb(db);
                                                                    setRenameInput(db);
                                                                }}
                                                                className="rename-btn"
                                                            >
                                                                Rename
                                                            </button>
                                                            <button
                                                                onClick={() => deleteDatabase(db)}
                                                                className="delete-btn"
                                                            >
                                                                Delete
                                                            </button>
                                                        </>
                                                    )}
                                                </div>
                                            </>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>

                <form onSubmit={handleSubmit}>
                    <textarea
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Enter SQL-like query...
e.g., CREATE TABLE users (id INT, name VARCHAR);
e.g., INSERT INTO users VALUES (1, 'Alice');
e.g., SELECT * FROM users;
e.g., SHOW LOGS;
e.g., DOWNLOAD LOGS;
e.g., CLEAR LOGS;"
                        rows={5}
                    />
                    <br />
                    <button type="submit">Execute in "{selectedDb}"</button>
                </form>

                {error && (
                    <div className="error-message">
                        <h3>Error</h3>
                        <pre>{error}</pre>
                    </div>
                )}

                {result && result.status === 'success' && (
                    <div className="result-container">
                        <h3>Result</h3>
                        {renderResult()}
                    </div>
                )}
            </header>
        </div>
    );
}

export default App;