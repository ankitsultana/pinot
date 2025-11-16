# Migration Plan: Material-UI v4 to shadcn/ui

## Overview
This document outlines the comprehensive migration plan for upgrading the Apache Pinot Controller UI from Material-UI v4 to shadcn/ui, which will provide a modern, accessible, and maintainable component library.

## Current State Analysis

### Technology Stack
- **React**: 16.13.1 (needs upgrade to 18+)
- **UI Library**: Material-UI v4 (@material-ui/core, @material-ui/icons, @material-ui/lab)
- **Styling**: Material-UI's `makeStyles`, `withStyles`, `createStyles`
- **Theme**: Material-UI theme provider with custom theme
- **TypeScript**: Already configured
- **Bundler**: Webpack 5
- **Routing**: React Router v5
- **Icons**: Material-UI Icons (@material-ui/icons)

### Usage Statistics
- **Material-UI imports**: 201 across 83 files
- **Custom components**: ~50+ components using Material-UI
- **Styling hooks**: Extensive use of `makeStyles` and `withStyles`

## Migration Prerequisites

### 1. React Version Upgrade
**Current**: React 16.13.1  
**Target**: React 18.2.0+

**Why**: shadcn/ui components require React 18+ for proper functionality and hooks support.

**Impact**:
- Update React and React-DOM to 18.2.0+
- Update React Router to v6 (compatible with React 18)
- Review breaking changes in React 18 migration guide
- Update lifecycle methods and refs if any

### 2. Build System Updates
**Current**: Webpack 5  
**Target**: Continue with Webpack 5 (or consider Vite for better DX)

**Changes Needed**:
- Add Tailwind CSS and PostCSS configuration
- Update webpack config for Tailwind CSS processing
- Add PostCSS loader to webpack

### 3. Styling System Migration
**Current**: Material-UI CSS-in-JS (makeStyles, withStyles)  
**Target**: Tailwind CSS + CSS Variables

**Changes Needed**:
- Install and configure Tailwind CSS
- Replace all Material-UI styling with Tailwind utility classes
- Set up CSS variables for theming (replacing Material-UI theme)

## Step-by-Step Migration Plan

### Phase 1: Setup & Infrastructure (Foundation)

#### Step 1.1: Upgrade React
```bash
npm install react@^18.2.0 react-dom@^18.2.0
npm install @types/react@^18.2.0 @types/react-dom@^18.2.0
```

**Tasks**:
- [ ] Update `package.json` dependencies
- [ ] Update `index.tsx` to use `ReactDOM.createRoot()` instead of `ReactDOM.render()`
- [ ] Test application still works with React 18
- [ ] Fix any React 18 breaking changes

#### Step 1.2: Upgrade React Router
```bash
npm install react-router-dom@^6.20.0
```

**Tasks**:
- [ ] Update `App.tsx` to use React Router v6 APIs (`Routes`, `Route`, `Navigate`)
- [ ] Replace `useHistory` with `useNavigate`
- [ ] Update all route configurations
- [ ] Test routing functionality

#### Step 1.3: Install Tailwind CSS and Dependencies
```bash
npm install -D tailwindcss@^3.4.0 postcss@^8.4.0 autoprefixer@^10.4.16
npm install tailwindcss postcss autoprefixer
npm install class-variance-authority clsx tailwind-merge
npm install lucide-react  # For icons (alternative to Material-UI icons)
```

**Tasks**:
- [ ] Initialize Tailwind config: `npx tailwindcss init -p`
- [ ] Create `tailwind.config.js` with proper content paths
- [ ] Create `postcss.config.js`
- [ ] Update webpack config to process Tailwind CSS
- [ ] Create main CSS file with Tailwind directives
- [ ] Set up utility functions (`cn` helper for class merging)

#### Step 1.4: Install shadcn/ui Dependencies
```bash
npm install @radix-ui/react-slot @radix-ui/react-dialog @radix-ui/react-dropdown-menu @radix-ui/react-select @radix-ui/react-tabs @radix-ui/react-toast @radix-ui/react-tooltip @radix-ui/react-popover @radix-ui/react-accordion
```

**Note**: shadcn/ui components will be installed individually using their CLI or manually copied to `components/ui/`.

#### Step 1.5: Configure Tailwind CSS
**File**: `tailwind.config.js`
```javascript
module.exports = {
  darkMode: ["class"],
  content: [
    "./app/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Map Material-UI colors to Tailwind
        primary: {
          // Use existing primary colors from theme/color/primary.ts
        },
        secondary: {
          // Use existing secondary colors from theme/color/secondary.ts
        },
      },
    },
  },
  plugins: [require("tailwindcss-animate")],
}
```

**Tasks**:
- [ ] Create `tailwind.config.js`
- [ ] Migrate color system from Material-UI theme to Tailwind config
- [ ] Set up CSS variables for theming
- [ ] Create `globals.css` with Tailwind directives and CSS variables

### Phase 2: Core Component Migration

#### Step 2.1: Set Up shadcn/ui Components Structure
**Directory**: `app/components/ui/`

**Initial Components to Install**:
- Button
- Dialog
- Input
- Select
- Table
- Tabs
- Toast (for notifications)
- Tooltip
- Accordion
- Card
- Badge/Chip
- Dropdown Menu
- Popover
- Sheet (sidebar replacement)

**Tasks**:
- [ ] Install shadcn/ui CLI: `npx shadcn-ui@latest init`
- [ ] Configure `components.json` for project structure
- [ ] Install core components one by one
- [ ] Test each component in isolation

#### Step 2.2: Create Utility Functions
**File**: `app/lib/utils.ts`
```typescript
import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}
```

#### Step 2.3: Replace Theme System
**Tasks**:
- [ ] Remove Material-UI `MuiThemeProvider` from `index.tsx`
- [ ] Replace with CSS variables-based theming
- [ ] Update `globals.css` with CSS variables for colors
- [ ] Create theme context if needed (for dark mode support)

### Phase 3: Component-by-Component Migration

#### Migration Strategy
1. **Start with Leaf Components** (components that don't depend on others)
2. **Work Upward** (move to parent components)
3. **Migrate Pages Last** (they depend on all component migrations)

#### Priority Order:
1. **Basic Components** (CustomButton, CustomDialog, CustomNotification)
2. **Layout Components** (Header, Sidebar, Layout)
3. **Data Display** (Table, TableToolbar, SimpleAccordion)
4. **Form Components** (CustomMultiSelect, SearchBar)
5. **Complex Components** (Query components, Operations)
6. **Pages** (HomePage, Query, TablesListingPage, etc.)

#### Step 3.1: Migrate Basic Components

**3.1.1 CustomButton → shadcn/ui Button**
- [ ] Replace `@material-ui/core/Button` with `@/components/ui/button`
- [ ] Convert `makeStyles` to Tailwind classes
- [ ] Update all usages
- [ ] Test functionality

**3.1.2 CustomDialog → shadcn/ui Dialog**
- [ ] Replace Material-UI Dialog with shadcn/ui Dialog
- [ ] Migrate DialogContent, DialogTitle, DialogActions
- [ ] Update styling
- [ ] Test all dialog usages

**3.1.3 CustomNotification → shadcn/ui Toast**
- [ ] Replace Material-UI Snackbar with shadcn/ui Toast
- [ ] Update notification context/provider
- [ ] Test notifications

#### Step 3.2: Migrate Layout Components

**3.2.1 Header Component**
- [ ] Replace `AppBar` with custom header using Tailwind
- [ ] Replace Material-UI `Box` with `div` and Tailwind classes
- [ ] Replace Material-UI `Paper` with shadcn/ui `Card` or custom
- [ ] Update styling
- [ ] Test responsive behavior

**3.2.2 Sidebar Component**
- [ ] Replace Material-UI `Drawer`/`List` with shadcn/ui `Sheet` or custom
- [ ] Replace `ListItem` with custom nav items
- [ ] Update styling and animations
- [ ] Test sidebar functionality

**3.2.3 Layout Component**
- [ ] Replace Material-UI `Grid` with Tailwind Grid/Flexbox
- [ ] Update layout structure
- [ ] Test layout responsiveness

#### Step 3.3: Migrate Data Display Components

**3.3.1 Table Component** (Complex - highest priority)
- [ ] Replace Material-UI Table with shadcn/ui Table
- [ ] Migrate TableHead, TableBody, TableRow, TableCell
- [ ] Replace TablePagination with custom pagination or shadcn/ui component
- [ ] Migrate all styling from `makeStyles` to Tailwind
- [ ] Update sorting, filtering, and pagination logic
- [ ] Test all table features

**3.3.2 TableToolbar**
- [ ] Replace Material-UI components with Tailwind/shadcn/ui
- [ ] Update search input styling

**3.3.3 SimpleAccordion**
- [ ] Replace Material-UI Accordion with shadcn/ui Accordion
- [ ] Update styling

#### Step 3.4: Migrate Form Components

**3.4.1 CustomMultiSelect**
- [ ] Replace Material-UI Select with shadcn/ui Select or Combobox
- [ ] Update styling

**3.4.2 SearchBar**
- [ ] Replace Material-UI TextField with shadcn/ui Input
- [ ] Update styling

**3.4.3 All Form Components in Operations/**
- [ ] Migrate each form component individually
- [ ] Replace TextField, Select, Checkbox, Switch, etc.
- [ ] Update form validation if needed

#### Step 3.5: Migrate Complex Components

**3.5.1 Query Components**
- [ ] Migrate QuerySideBar, TimeseriesChart, etc.
- [ ] Update chart libraries if needed (echarts should work as-is)

**3.5.2 Operations Components**
- [ ] Migrate all components in `app/components/Homepage/Operations/`
- [ ] Replace form components, dialogs, buttons

**3.5.3 Zookeeper Components**
- [ ] Replace MaterialTree with custom tree component
- [ ] Use shadcn/ui Accordion or custom tree implementation

#### Step 3.6: Migrate Pages

**3.6.1 HomePage**
- [ ] Replace Grid, Paper with Tailwind/shadcn/ui components
- [ ] Update all child component usages

**3.6.2 Query Page**
- [ ] Update query interface
- [ ] Replace CodeMirror styling if needed

**3.6.3 All Other Pages**
- [ ] Migrate page by page
- [ ] Replace Material-UI components with Tailwind equivalents
- [ ] Test each page thoroughly

### Phase 4: Icon Migration

#### Step 4.1: Replace Material-UI Icons
**Current**: `@material-ui/icons`  
**Target**: `lucide-react` (recommended) or custom SVG icons

**Tasks**:
- [ ] Identify all Material-UI icon usages (grep for `@material-ui/icons`)
- [ ] Create mapping document (Material-UI icon → Lucide icon)
- [ ] Replace icons one by one
- [ ] Update icon sizes and styling
- [ ] Test visual appearance

**Popular Mappings**:
- `Menu` → `Menu` (lucide-react)
- `ArrowDropDown` → `ChevronDown`
- `ArrowDropUp` → `ChevronUp`
- `FirstPage` → `ChevronsLeft`
- `LastPage` → `ChevronsRight`
- `AccountCircle` → `User`
- etc.

### Phase 5: Styling Cleanup

#### Step 5.1: Remove Material-UI Dependencies
```bash
npm uninstall @material-ui/core @material-ui/icons @material-ui/lab
```

**Tasks**:
- [ ] Verify no Material-UI imports remain (grep check)
- [ ] Remove Material-UI theme files (`app/theme/`)
- [ ] Remove Material-UI-specific CSS from `styles.css`
- [ ] Clean up unused dependencies

#### Step 5.2: Update Global Styles
**File**: `app/styles/styles.css`

**Tasks**:
- [ ] Remove Material-UI-specific styles
- [ ] Keep CodeMirror styles (not Material-UI specific)
- [ ] Add Tailwind base styles if needed
- [ ] Clean up unused CSS

#### Step 5.3: Remove Material-UI Styling System
**Tasks**:
- [ ] Remove all `makeStyles` hooks
- [ ] Remove all `withStyles` HOCs
- [ ] Remove `createStyles` imports
- [ ] Convert all inline styles to Tailwind where possible

### Phase 6: Testing & Polish

#### Step 6.1: Visual Testing
- [ ] Test all pages for visual consistency
- [ ] Verify responsive design on mobile/tablet/desktop
- [ ] Check dark mode if implemented
- [ ] Verify accessibility (keyboard navigation, screen readers)

#### Step 6.2: Functional Testing
- [ ] Test all forms and form validation
- [ ] Test all dialogs and modals
- [ ] Test table sorting, filtering, pagination
- [ ] Test navigation and routing
- [ ] Test authentication flow
- [ ] Test all CRUD operations

#### Step 6.3: Performance Testing
- [ ] Check bundle size reduction (Material-UI is large)
- [ ] Verify no performance regressions
- [ ] Test initial load time
- [ ] Check runtime performance

#### Step 6.4: Code Quality
- [ ] Run ESLint and fix issues
- [ ] Remove unused imports
- [ ] Update TypeScript types
- [ ] Add proper type definitions for shadcn/ui components

### Phase 7: Documentation & Cleanup

#### Step 7.1: Update Documentation
- [ ] Update README with new tech stack
- [ ] Document component usage patterns
- [ ] Update development setup instructions
- [ ] Create component migration guide for future reference

#### Step 7.2: Final Cleanup
- [ ] Remove all TODO comments
- [ ] Clean up commented-out code
- [ ] Update package.json scripts if needed
- [ ] Update build configuration documentation

## Key Considerations

### Breaking Changes to Watch For
1. **React 18**: 
   - `ReactDOM.render()` → `ReactDOM.createRoot()`
   - Automatic batching changes
   - Stricter type checking

2. **React Router v6**:
   - `Switch` → `Routes`
   - `Route` component prop → `element` prop
   - `useHistory` → `useNavigate`
   - Route matching changes

3. **Material-UI to shadcn/ui**:
   - Different prop APIs
   - Different styling approach (utility classes vs CSS-in-JS)
   - Component structure differences

### Risk Mitigation
1. **Incremental Migration**: Migrate component by component, test frequently
2. **Feature Branch**: Work in a dedicated branch, merge incrementally
3. **Testing**: Test each migrated component thoroughly before moving to next
4. **Rollback Plan**: Keep Material-UI dependencies until migration is complete
5. **Team Communication**: Keep team informed of progress and breaking changes

### Estimated Timeline
- **Phase 1 (Setup)**: 1-2 weeks
- **Phase 2 (Core Components)**: 1-2 weeks
- **Phase 3 (Component Migration)**: 4-6 weeks
- **Phase 4 (Icons)**: 1 week
- **Phase 5 (Cleanup)**: 1 week
- **Phase 6 (Testing)**: 2 weeks
- **Phase 7 (Documentation)**: 1 week

**Total Estimated Time**: 10-15 weeks (depending on team size and complexity)

### Benefits of Migration
1. **Modern UI**: shadcn/ui provides modern, accessible components
2. **Smaller Bundle**: Tailwind CSS + shadcn/ui is typically smaller than Material-UI
3. **Better Performance**: Utility-first CSS is more performant
4. **Customization**: Easier to customize with Tailwind CSS
5. **Type Safety**: Better TypeScript integration
6. **Accessibility**: Built on Radix UI primitives (excellent a11y)
7. **Maintainability**: Copy components into your codebase for full control

### Resources
- [shadcn/ui Documentation](https://ui.shadcn.com/)
- [Tailwind CSS Documentation](https://tailwindcss.com/docs)
- [Radix UI Documentation](https://www.radix-ui.com/)
- [React 18 Migration Guide](https://react.dev/blog/2022/03/08/react-18-upgrade-guide)
- [React Router v6 Migration Guide](https://reactrouter.com/en/main/upgrading/v5)

