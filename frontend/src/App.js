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

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setResult(null);

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
    }

    return (
        <div className="App">
            <header className="App-header">
                <h1>Database Query Interface</h1>

                {/* Database Selection */}
                <div className="db-controls">
                    <div className="db-selector">
                        <label>Current Database: </label>
                        <select
                            value={selectedDb}
                            onChange={(e) => setSelectedDb(e.target.value)}
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
                                        <span>{db}</span>
                                        {db !== 'default' && (
                                            <button
                                                onClick={() => deleteDatabase(db)}
                                                className="delete-btn"
                                            >
                                                Delete
                                            </button>
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
e.g., SELECT * FROM users;"
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