# Quick Start Guide: Beginning the Migration

This guide provides step-by-step instructions for starting the migration from Material-UI v4 to shadcn/ui.

## Prerequisites

Before starting, ensure you have:
- Node.js 16+ installed
- npm or yarn package manager
- Access to the repository
- Understanding of React, TypeScript, and Tailwind CSS basics

## Step 1: Create a Migration Branch

```bash
git checkout -b migrate-to-shadcn-ui
```

## Step 2: Upgrade React (Foundation)

### 2.1 Update Dependencies

```bash
npm install react@^18.2.0 react-dom@^18.2.0
npm install @types/react@^18.2.0 @types/react-dom@^18.2.0
npm install react-router-dom@^6.20.0
```

### 2.2 Update index.tsx

**Before** (React 16):
```typescript
import ReactDOM from "react-dom";

ReactDOM.render(
    <HashRouter>
        <MuiThemeProvider theme={theme}>
            {/* ... */}
        </MuiThemeProvider>
    </HashRouter>,
    document.getElementById('app')
);
```

**After** (React 18):
```typescript
import { createRoot } from "react-dom/client";

const container = document.getElementById('app');
const root = createRoot(container!);

root.render(
    <HashRouter>
        {/* MuiThemeProvider removed - will be replaced with CSS variables */}
        {/* ... */}
    </HashRouter>
);
```

### 2.3 Update React Router

Update `App.tsx` to use React Router v6 APIs:

**Before** (v5):
```typescript
import { Switch, Route, Redirect, useHistory } from 'react-router-dom';

<Switch>
  <Route exact path="/" component={HomePage} />
  <Route path="/login">
    <Redirect to="/" />
  </Route>
</Switch>
```

**After** (v6):
```typescript
import { Routes, Route, Navigate, useNavigate } from 'react-router-dom';

<Routes>
  <Route path="/" element={<HomePage />} />
  <Route path="/login" element={<Navigate to="/" />} />
</Routes>
```

## Step 3: Install Tailwind CSS

### 3.1 Install Dependencies

```bash
npm install -D tailwindcss@^3.4.0 postcss@^8.4.0 autoprefixer@^10.4.16
npm install clsx tailwind-merge class-variance-authority
```

### 3.2 Initialize Tailwind

```bash
npx tailwindcss init -p
```

This creates:
- `tailwind.config.js`
- `postcss.config.js`

### 3.3 Configure Tailwind

Update `tailwind.config.js`:

```javascript
/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: ["class"],
  content: [
    "./app/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Migrate your Material-UI theme colors here
        primary: {
          DEFAULT: '#4285f4', // Your primary color
          light: '#...',
          dark: '#...',
        },
        secondary: {
          DEFAULT: '#...',
          // ...
        },
      },
      fontFamily: {
        sans: ["'Source Sans Pro', sans-serif"],
      },
    },
  },
  plugins: [require("tailwindcss-animate")],
}
```

### 3.4 Create globals.css

Create `app/styles/globals.css`:

```css
@tailwind base;
@tailwind components;
@tailwind utilities;

@layer base {
  :root {
    --background: 0 0% 100%;
    --foreground: 222.2 84% 4.9%;
    --primary: 221.2 83.2% 53.3%;
    --primary-foreground: 210 40% 98%;
    --secondary: 210 40% 96.1%;
    --secondary-foreground: 222.2 47.4% 11.2%;
    /* Add more CSS variables as needed */
  }
}

@layer base {
  * {
    @apply border-border;
  }
  body {
    @apply bg-background text-foreground;
  }
}
```

### 3.5 Import globals.css

Add to `app/index.tsx`:
```typescript
import './styles/globals.css';
```

### 3.6 Update Webpack Config

Add PostCSS loader to `webpack.config.js`:

```javascript
module: {
  rules: [
    // ... existing rules
    {
      test: /\.css$/i,
      use: [
        'style-loader',
        'css-loader',
        'postcss-loader', // Add this
      ],
    },
  ],
}
```

## Step 4: Install shadcn/ui

### 4.1 Create Components Directory

```bash
mkdir -p app/components/ui
mkdir -p app/lib
```

### 4.2 Create Utility Function

Create `app/lib/utils.ts`:

```typescript
import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}
```

### 4.3 Update tsconfig.json

Add path alias:

```json
{
  "compilerOptions": {
    "baseUrl": ".",
    "paths": {
      "@/*": ["./app/*"]
    }
  }
}
```

### 4.4 Initialize shadcn/ui

```bash
npx shadcn-ui@latest init
```

This will:
- Create `components.json` configuration file
- Set up the project structure

Follow the prompts:
- Style: Default
- Base color: Slate (or your preference)
- CSS variables: Yes

### 4.5 Install Core Components

```bash
npx shadcn-ui@latest add button
npx shadcn-ui@latest add dialog
npx shadcn-ui@latest add input
npx shadcn-ui@latest add select
npx shadcn-ui@latest add table
npx shadcn-ui@latest add tabs
npx shadcn-ui@latest add toast
npx shadcn-ui@latest add tooltip
npx shadcn-ui@latest add accordion
npx shadcn-ui@latest add card
npx shadcn-ui@latest add badge
npx shadcn-ui@latest add dropdown-menu
npx shadcn-ui@latest add sheet
```

## Step 5: Create Your First Migration

Let's migrate a simple component as an example: `CustomButton`

### 5.1 Before (Material-UI)

```typescript
import { Button, makeStyles, Tooltip } from '@material-ui/core';

const useStyles = makeStyles((theme) => ({
  button: {
    margin: theme.spacing(1),
    textTransform: 'none'
  }
}));

export default function CustomButton({ children, onClick, isDisabled, tooltipTitle, enableTooltip }) {
  const classes = useStyles();
  
  return (
    <Tooltip title={tooltipTitle} disableHoverListener={!enableTooltip} placement="top" arrow>
      <Button
        variant="contained"
        color="primary"
        className={classes.button}
        size="small"
        onClick={onClick}
        disabled={isDisabled}
      >
        {children}
      </Button>
    </Tooltip>
  );
}
```

### 5.2 After (shadcn/ui)

```typescript
import { Button } from '@/components/ui/button';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';

type Props = {
  children: React.ReactNode;
  onClick: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void;
  isDisabled?: boolean;
  tooltipTitle?: string;
  enableTooltip?: boolean;
};

export default function CustomButton({
  children,
  onClick,
  isDisabled,
  tooltipTitle = '',
  enableTooltip = false
}: Props) {
  const button = (
    <Button
      variant="default"
      size="sm"
      onClick={onClick}
      disabled={isDisabled}
      className={cn("m-1 normal-case")}
    >
      {children}
    </Button>
  );

  if (enableTooltip && tooltipTitle) {
    return (
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            {button}
          </TooltipTrigger>
          <TooltipContent>
            <p>{tooltipTitle}</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    );
  }

  return button;
}
```

## Step 6: Install Icons

Replace Material-UI icons with Lucide React:

```bash
npm install lucide-react
```

### Example Icon Replacement

**Before**:
```typescript
import MenuIcon from '@material-ui/icons/Menu';
```

**After**:
```typescript
import { Menu } from 'lucide-react';
```

## Step 7: Test Your Changes

1. **Start dev server**:
```bash
npm run dev
```

2. **Test the migrated component**:
   - Navigate to a page that uses CustomButton
   - Verify it looks correct
   - Test all interactions (click, hover, disabled state, tooltip)

3. **Check for console errors**:
   - Open browser DevTools
   - Check Console tab for errors
   - Fix any TypeScript or runtime errors

## Step 8: Continue Migration

Follow this pattern for each component:

1. **Identify the component** to migrate
2. **Find all usages** (grep for component name)
3. **Replace Material-UI imports** with shadcn/ui equivalents
4. **Convert styles** from `makeStyles`/`withStyles` to Tailwind classes
5. **Test thoroughly** before moving to next component
6. **Update types** if needed

## Common Patterns

### Replacing Material-UI Box

**Before**:
```typescript
<Box display="flex" alignItems="center" marginY={2}>
  Content
</Box>
```

**After**:
```typescript
<div className="flex items-center my-2">
  Content
</div>
```

### Replacing Material-UI Grid

**Before**:
```typescript
<Grid container spacing={2}>
  <Grid item xs={12} md={6}>
    Content
  </Grid>
</Grid>
```

**After**:
```typescript
<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
  <div>Content</div>
</div>
```

### Replacing Material-UI Paper

**Before**:
```typescript
<Paper elevation={2} className={classes.paper}>
  Content
</Paper>
```

**After**:
```typescript
import { Card } from '@/components/ui/card';

<Card className="shadow-md p-4">
  Content
</Card>
```

## Next Steps

1. Review the full [MIGRATION_PLAN.md](./MIGRATION_PLAN.md) for comprehensive details
2. Use [MIGRATION_CHECKLIST.md](./MIGRATION_CHECKLIST.md) to track progress
3. Start with leaf components (components with no dependencies)
4. Work your way up to parent components and pages
5. Test frequently and commit often

## Troubleshooting

### Tailwind classes not applying?
- Check `tailwind.config.js` content paths
- Ensure PostCSS loader is in webpack config
- Verify `globals.css` is imported

### TypeScript errors?
- Ensure `@/*` path alias is configured in `tsconfig.json`
- Update type imports from Material-UI to shadcn/ui

### Components not rendering?
- Check browser console for errors
- Verify all dependencies are installed
- Ensure shadcn/ui components are properly installed

### Styling looks different?
- This is expected! Adjust Tailwind classes to match desired appearance
- Use Tailwind's spacing/sizing scale: `m-1`, `p-2`, `w-full`, etc.
- Customize in `tailwind.config.js` if needed

## Resources

- [shadcn/ui Documentation](https://ui.shadcn.com/)
- [Tailwind CSS Documentation](https://tailwindcss.com/docs)
- [React 18 Upgrade Guide](https://react.dev/blog/2022/03/08/react-18-upgrade-guide)
- [React Router v6 Upgrade Guide](https://reactrouter.com/en/main/upgrading/v5)
- [Lucide Icons](https://lucide.dev/icons/)

