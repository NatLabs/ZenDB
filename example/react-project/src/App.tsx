import './App.css';
import motokoLogo from './assets/motoko_moving.png';
import motokoShadowLogo from './assets/motoko_shadow.png';
import reactLogo from './assets/react.svg';
import viteLogo from './assets/vite.svg';
import { useQueryCall, useUpdateCall } from '@ic-reactor/react';
import { TxsTable } from './pages/TxsTable';
import { QueryClient, QueryClientProvider } from 'react-query';

function App() {
    const queryClient = new QueryClient();
    return (
        // <div className="App">
        <QueryClientProvider client={queryClient}>
            <TxsTable />
        </QueryClientProvider>
    );
}

export default App;