import { render, screen } from '@testing-library/react';
import App from '../../../../../../Downloads/VK_DB_SIRIUS-master/VK_DB_SIRIUS-master/frontend/src/App';

test('renders learn react link', () => {
  render(<App />);
  const linkElement = screen.getByText(/learn react/i);
  expect(linkElement).toBeInTheDocument();
});
