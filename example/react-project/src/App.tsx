import './App.css';
import motokoLogo from './assets/motoko_moving.png';
import motokoShadowLogo from './assets/motoko_shadow.png';
import reactLogo from './assets/react.svg';
import viteLogo from './assets/vite.svg';
import { useQueryCall, useUpdateCall } from '@ic-reactor/react';
import { TxsTable } from './pages/TxsTable';
import { QueryClientProvider } from 'react-query';
import { queryClient } from './utils/react-query-client';
import { Switch, Route } from 'wouter';

function App() {
    return (
        // <div className="App">
        <QueryClientProvider client={queryClient}>
            <Switch>
                <Route path="/" component={TxsTable} />
            </Switch>
        </QueryClientProvider>
    );
}

export default App;
