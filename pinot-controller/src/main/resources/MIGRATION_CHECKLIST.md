# Migration Checklist: Material-UI v4 → shadcn/ui

## Quick Reference Checklist

### Pre-Migration Assessment
- [x] Analyze current tech stack (React 16.13.1, Material-UI v4)
- [x] Identify all Material-UI imports (201 across 83 files)
- [x] Document component dependencies
- [x] Create migration plan

### Phase 1: Foundation Setup
- [ ] Upgrade React to 18.2.0+
- [ ] Upgrade React-DOM to 18.2.0+
- [ ] Update React types (@types/react, @types/react-dom)
- [ ] Update index.tsx to use createRoot()
- [ ] Upgrade React Router to v6
- [ ] Migrate React Router APIs (Switch→Routes, useHistory→useNavigate)
- [ ] Install Tailwind CSS
- [ ] Configure Tailwind config
- [ ] Set up PostCSS
- [ ] Update webpack config for Tailwind
- [ ] Install shadcn/ui dependencies (Radix UI primitives)
- [ ] Install utility libraries (clsx, tailwind-merge, class-variance-authority)
- [ ] Install icon library (lucide-react)
- [ ] Set up CSS variables for theming
- [ ] Create globals.css with Tailwind directives

### Phase 2: Core Component Setup
- [ ] Install shadcn/ui CLI
- [ ] Configure components.json
- [ ] Install Button component
- [ ] Install Dialog component
- [ ] Install Input component
- [ ] Install Select component
- [ ] Install Table component
- [ ] Install Tabs component
- [ ] Install Toast component
- [ ] Install Tooltip component
- [ ] Install Accordion component
- [ ] Install Card component
- [ ] Install Badge/Chip component
- [ ] Install Dropdown Menu component
- [ ] Install Popover component
- [ ] Install Sheet component (for sidebar)
- [ ] Create utils.ts with cn() helper

### Phase 3: Theme Migration
- [ ] Remove MuiThemeProvider from index.tsx
- [ ] Migrate color system to Tailwind config
- [ ] Set up CSS variables for colors
- [ ] Update globals.css with theme variables
- [ ] Test theme application

### Phase 4: Component Migration

#### Basic Components
- [ ] CustomButton → shadcn/ui Button
- [ ] CustomDialog → shadcn/ui Dialog
- [ ] CustomNotification → shadcn/ui Toast
- [ ] Confirm → shadcn/ui Dialog (confirmation variant)

#### Layout Components
- [ ] Header → Custom header with Tailwind
- [ ] SideBar → shadcn/ui Sheet or custom
- [ ] Layout → Tailwind Grid/Flexbox
- [ ] Breadcrumbs → Custom or shadcn/ui Breadcrumb

#### Data Display
- [ ] Table → shadcn/ui Table
- [ ] TableToolbar → Custom with Tailwind
- [ ] SimpleAccordion → shadcn/ui Accordion
- [ ] TablePagination → Custom pagination

#### Form Components
- [ ] CustomMultiSelect → shadcn/ui Select/Combobox
- [ ] SearchBar → shadcn/ui Input
- [ ] TimezoneSelector → shadcn/ui Select
- [ ] StatusFilter → shadcn/ui Select/Combobox
- [ ] TaskStatusFilter → shadcn/ui Select/Combobox
- [ ] All form components in Operations/ directory

#### Complex Components
- [ ] QuerySideBar → Custom with shadcn/ui components
- [ ] TimeseriesChart → Keep echarts, update wrapper
- [ ] MaterialTree → Custom tree with shadcn/ui Accordion
- [ ] All Operations components (20+ components)

#### Pages
- [ ] HomePage
- [ ] Query page
- [ ] TimeseriesQueryPage
- [ ] TablesListingPage
- [ ] TableDetails page
- [ ] InstanceListingPage
- [ ] InstanceDetails page
- [ ] SchemaPageDetails
- [ ] SegmentDetails
- [ ] TenantDetails
- [ ] TenantsListingPage
- [ ] UserPage
- [ ] LoginPage
- [ ] ZookeeperPage
- [ ] TaskQueue
- [ ] TaskDetail
- [ ] SubTaskDetail
- [ ] MinionTaskManager

### Phase 5: Icon Migration
- [ ] Audit all Material-UI icon usages
- [ ] Create icon mapping document
- [ ] Replace Menu icon
- [ ] Replace ArrowDropDown/ArrowDropUp icons
- [ ] Replace FirstPage/LastPage icons
- [ ] Replace KeyboardArrowLeft/Right icons
- [ ] Replace AccountCircle icon
- [ ] Replace all other icons (50+ icons)
- [ ] Update icon sizing and styling

### Phase 6: Styling Cleanup
- [ ] Remove all makeStyles hooks
- [ ] Remove all withStyles HOCs
- [ ] Remove createStyles imports
- [ ] Convert inline styles to Tailwind classes
- [ ] Update styles.css (remove Material-UI specific styles)
- [ ] Remove Material-UI theme files

### Phase 7: Dependency Cleanup
- [ ] Verify no Material-UI imports remain (grep check)
- [ ] Uninstall @material-ui/core
- [ ] Uninstall @material-ui/icons
- [ ] Uninstall @material-ui/lab
- [ ] Remove unused Material-UI dependencies
- [ ] Clean up package.json

### Phase 8: Testing
- [ ] Test all pages visually
- [ ] Test responsive design (mobile/tablet/desktop)
- [ ] Test dark mode (if implemented)
- [ ] Test accessibility (keyboard nav, screen readers)
- [ ] Test all forms and validation
- [ ] Test all dialogs/modals
- [ ] Test table features (sort, filter, pagination)
- [ ] Test navigation and routing
- [ ] Test authentication flow
- [ ] Test all CRUD operations
- [ ] Performance testing (bundle size, load time)
- [ ] Cross-browser testing

### Phase 9: Code Quality
- [ ] Run ESLint and fix all issues
- [ ] Remove unused imports
- [ ] Update TypeScript types
- [ ] Add proper type definitions
- [ ] Remove commented code
- [ ] Remove TODO comments
- [ ] Update code comments

### Phase 10: Documentation
- [ ] Update README.md
- [ ] Document new tech stack
- [ ] Update development setup
- [ ] Create component usage guide
- [ ] Document migration process (for future reference)
- [ ] Update API documentation if needed

## Component Mapping Reference

### Material-UI → shadcn/ui Equivalents

| Material-UI | shadcn/ui | Notes |
|------------|-----------|-------|
| Button | Button | Similar API |
| Dialog | Dialog | Similar structure |
| TextField | Input | Different API |
| Select | Select | Different API, use Combobox for search |
| Table | Table | Similar structure |
| Tabs | Tabs | Similar API |
| Snackbar | Toast | Different implementation |
| Tooltip | Tooltip | Similar API |
| Accordion | Accordion | Similar API |
| Paper | Card | Similar concept |
| Chip | Badge | Similar but different |
| Menu | DropdownMenu | Different API |
| Drawer | Sheet | For sidebars/modals |
| Grid | Tailwind Grid | Use Tailwind classes |
| Box | div + Tailwind | Use Tailwind classes |
| Typography | h1-h6, p + Tailwind | Use semantic HTML |
| AppBar | Custom header | Use Tailwind |
| List | Custom nav | Use Tailwind + shadcn/ui components |

### Icon Mapping

| Material-UI Icon | Lucide React Icon |
|-----------------|-------------------|
| Menu | Menu |
| ArrowDropDown | ChevronDown |
| ArrowDropUp | ChevronUp |
| FirstPage | ChevronsLeft |
| LastPage | ChevronsRight |
| KeyboardArrowLeft | ChevronLeft |
| KeyboardArrowRight | ChevronRight |
| AccountCircle | User |
| Search | Search |
| Close | X |
| Check | Check |
| Delete | Trash2 |
| Edit | Pencil |
| Add | Plus |
| Remove | Minus |
| Settings | Settings |
| MoreVert | MoreVertical |
| ExpandMore | ChevronDown |
| ExpandLess | ChevronUp |

## Critical Files to Update

1. **package.json** - Update dependencies
2. **tsconfig.json** - May need path aliases for shadcn/ui
3. **webpack.config.js** - Add Tailwind CSS processing
4. **tailwind.config.js** - Configure Tailwind
5. **postcss.config.js** - Configure PostCSS
6. **app/index.tsx** - Update React 18 root, remove MuiThemeProvider
7. **app/App.tsx** - Update React Router v6 APIs
8. **app/styles/styles.css** - Replace with Tailwind globals
9. **app/lib/utils.ts** - Create cn() utility
10. **app/components/ui/** - shadcn/ui components directory

## Estimated Effort by Phase

- Phase 1: Foundation - 1-2 weeks
- Phase 2: Core Components - 1-2 weeks  
- Phase 3: Theme Migration - 1 week
- Phase 4: Component Migration - 4-6 weeks
- Phase 5: Icons - 1 week
- Phase 6: Styling Cleanup - 1 week
- Phase 7: Dependency Cleanup - 1-2 days
- Phase 8: Testing - 2 weeks
- Phase 9: Code Quality - 1 week
- Phase 10: Documentation - 1 week

**Total: 10-15 weeks**

