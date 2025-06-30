import React, { useState } from 'react';
import './App.css';

function App() {
    const [query, setQuery] = useState('');
    const [result, setResult] = useState(null);
    const [error, setError] = useState('');

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setResult(null);

        const API_URL = '/api/query';

        try {
            const requestBody = JSON.stringify({ query: query });
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
                    <button type="submit">Execute</button>
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