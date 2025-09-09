# NovaMeet Frontend

A modern, enterprise-grade React TypeScript frontend for the NovaMeet scheduling platform.

## 🚀 Tech Stack

- **React 18** with TypeScript
- **Vite** for fast development and building
- **Material-UI (MUI)** for the design system
- **Framer Motion** for smooth animations
- **TanStack Query** for data fetching and caching
- **Zustand** for state management
- **React Hook Form** for form handling
- **React Router** for navigation

## 📁 Project Structure

```
src/
├── api/                    # API client and query configurations
├── components/
│   ├── core/              # Reusable core components
│   └── layout/            # Layout components (Header, Sidebar, etc.)
├── hooks/                 # Custom React hooks
├── pages/                 # Top-level page components
├── router/                # Routing configuration
├── store/                 # Global state management
├── theme/                 # MUI theme configuration
├── types/                 # TypeScript type definitions
├── utils/                 # Utility functions
└── [modules]/             # Module-specific directories
    ├── users/
    ├── events/
    ├── availability/
    ├── integrations/
    ├── notifications/
    ├── contacts/
    └── workflows/
```

## 🎨 Design System

The project uses a comprehensive design system built on Material-UI with:

- **Color Palette**: Professional indigo primary with pink accent
- **Typography**: Inter font family with consistent scales
- **Spacing**: 8px grid system
- **Components**: Customized MUI components with consistent styling
- **Animations**: Smooth Framer Motion animations throughout

## 🛠 Development Guidelines

### Module Structure

Each module should follow this structure:

```
src/[module]/
├── pages/                 # Module pages
├── components/            # Module-specific components
├── hooks/                 # Module-specific hooks
├── api/                   # Module API services
├── types/                 # Module type definitions
├── utils/                 # Module utilities
├── routes.tsx             # Module routing
└── index.ts               # Module exports
```

### Code Standards

- Use TypeScript strictly (no `any` types)
- Follow the established component patterns
- Use the core design system components
- Implement proper error handling
- Add loading states for async operations
- Use React Hook Form for forms
- Implement proper accessibility

### API Integration

- Use TanStack Query for all API calls
- Follow the established query key patterns
- Implement proper error handling
- Use the centralized API client
- Cache data appropriately

## 🚦 Getting Started

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Set up environment variables:**
   ```bash
   cp .env.example .env
   ```

3. **Start development server:**
   ```bash
   npm run dev
   ```

4. **Other commands:**
   ```bash
   npm run build          # Build for production
   npm run lint           # Run ESLint
   npm run lint:fix       # Fix ESLint errors
   npm run type-check     # Run TypeScript checks
   npm run format         # Format code with Prettier
   npm test               # Run tests
   ```

## 📋 Module Development Checklist

When developing a module, ensure you:

- [ ] Follow the established directory structure
- [ ] Use the core design system components
- [ ] Implement proper TypeScript types
- [ ] Add loading and error states
- [ ] Use TanStack Query for API calls
- [ ] Follow the established routing patterns
- [ ] Add proper form validation
- [ ] Implement responsive design
- [ ] Add smooth animations where appropriate
- [ ] Test your components thoroughly

## 🎯 Key Features

- **Responsive Design**: Works seamlessly on desktop, tablet, and mobile
- **Dark/Light Mode**: Toggle between themes
- **Real-time Updates**: Live data synchronization
- **Offline Support**: Graceful handling of network issues
- **Accessibility**: WCAG compliant components
- **Performance**: Optimized with lazy loading and caching
- **Security**: Secure authentication and authorization

## 🔧 Configuration

The project uses several configuration files:

- `tsconfig.json` - TypeScript configuration
- `vite.config.ts` - Vite build configuration
- `.eslintrc.cjs` - ESLint rules
- `.prettierrc` - Code formatting rules

## 📚 Resources

- [Material-UI Documentation](https://mui.com/)
- [Framer Motion Documentation](https://www.framer.com/motion/)
- [TanStack Query Documentation](https://tanstack.com/query/)
- [React Hook Form Documentation](https://react-hook-form.com/)
- [Zustand Documentation](https://github.com/pmndrs/zustand)