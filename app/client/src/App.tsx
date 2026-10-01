import { createBrowserRouter, RouterProvider, NavLink, Outlet } from 'react-router';
import { useState } from 'react';
import { Badge, Button, Sheet, SheetContent, SheetHeader, SheetTitle, useIsMobile } from '@databricks/appkit-ui/react';
import { Menu, ShieldAlert } from 'lucide-react';
import { FilaPage } from './pages/FilaPage';
import { RedePage } from './pages/RedePage';
import { GeniePage } from './pages/GeniePage';
import { useIdentidade } from './lib/identidade';

const navLinkClass = ({ isActive }: { isActive: boolean }) =>
  `px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
    isActive ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
  }`;

const mobileNavLinkClass = ({ isActive }: { isActive: boolean }) =>
  `block px-3 py-2 rounded-md text-sm font-medium transition-colors ${
    isActive ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
  }`;

type NavLinkClassFn = (props: { isActive: boolean }) => string;

function NavLinks({ className, linkClass, onClick }: { className?: string; linkClass: NavLinkClassFn; onClick?: () => void }) {
  return (
    <nav className={className}>
      <NavLink to="/" end className={linkClass} onClick={onClick}>Fila de risco</NavLink>
      <NavLink to="/rede" className={linkClass} onClick={onClick}>Rede de vínculos</NavLink>
      <NavLink to="/genie" className={linkClass} onClick={onClick}>Genie Agent</NavLink>
    </nav>
  );
}

function Layout() {
  const isMobile = useIsMobile();
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const eu = useIdentidade();
  const navAberta = isMobile && mobileNavOpen;   // fecha sozinha ao passar para desktop

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="border-b px-4 md:px-6 py-3 flex items-center gap-4">
        <div className="flex items-center gap-2">
          <ShieldAlert className="h-5 w-5 text-primary" />
          <h1 className="text-lg font-semibold text-foreground">Cielo · PLD em Grafos</h1>
        </div>
        <NavLinks className="hidden md:flex gap-1" linkClass={navLinkClass} />
        <div className="ml-auto hidden md:block">
          <Badge variant="secondary">{eu?.email ?? 'usuário autenticado'}</Badge>
        </div>
        <div className="ml-auto md:hidden">
          <Sheet open={navAberta} onOpenChange={setMobileNavOpen}>
            <Button variant="ghost" size="icon" onClick={() => setMobileNavOpen(true)}>
              <Menu className="h-5 w-5" />
              <span className="sr-only">Abrir navegação</span>
            </Button>
            <SheetContent side="left">
              <SheetHeader>
                <SheetTitle>Navegação</SheetTitle>
              </SheetHeader>
              <NavLinks className="flex flex-col gap-1" linkClass={mobileNavLinkClass} onClick={() => setMobileNavOpen(false)} />
            </SheetContent>
          </Sheet>
        </div>
      </header>

      <main className="flex-1 p-4 md:p-6">
        <Outlet />
      </main>
      <footer className="border-t px-4 md:px-6 py-2 text-xs text-muted-foreground">
        Demonstração com dados sintéticos · catálogo <code>cielo_pld</code> · scores do modelo <code>cielo_pld.gold.modelo_risco_pld_ec@champion</code>
      </footer>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <FilaPage /> },
      { path: '/rede', element: <RedePage /> },
      { path: '/genie', element: <GeniePage /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
